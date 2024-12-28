using System;
using System.Threading;
using Npgsql;
using adkuDBInterface.Model;

namespace adkuDBInterface.PG
{
    class PGDependency
    {
        // Внимание! для работы уведомлений необходимо чтобы в базе для соотв. таблицы был создан триггер с вызовом adku_nitify() (см alarm)
        string _cs;
        string _watchSQL;
        bool _state = false;
        Thread _th;
        Thread _thWD;
        Thread _dth;
        double _lastWatch = DateTime.Now.ToOADate();
        double _lastFire = DateTime.Now.ToOADate();
        public event QueryChangeHandler onChange;



        private void NotificationSupportHelper(Object sender, NpgsqlNotificationEventArgs e)
        {
            
            if (onChange != null && _watchSQL.ToLower().Contains(e.Payload.ToLower()))
            {
                _lastFire = DateTime.Now.ToOADate();
                // задержка при массовом обновлении чтобы не сыпать сообщениями попусту
                if (_dth != null)
                {
                    _dth.Interrupt();
                    _dth.Join();
                }
                _dth = new Thread(() =>
                {
                    try
                    {
                        Thread.Sleep(1000);
                        onChange(sender, e.Payload);
                    }
                    catch (Exception e)
                    {
                        //Console.WriteLine(e.Message);
                    }
                });
                _dth.Start();
            }
        }


        private void Do()
        {
            try
            {
                while (_state)

                    try
                    {
                        using (var conn = new NpgsqlConnection(_cs))
                        {
                            conn.Open();
                            conn.Notification += NotificationSupportHelper;
                            //Console.WriteLine("open");

                            using (var com = new NpgsqlCommand("listen adku;", conn))
                            {
                                com.ExecuteNonQuery();

                            }
                            //Console.WriteLine("listen");

                            try
                            {
                                while (_state)
                                {
                                    conn.Wait(5000);   // Thread will block here
                                    _lastWatch = DateTime.Now.ToOADate();
                                    // проверка соединения
                                    using (var com = new NpgsqlCommand("select 1;", conn)) { com.ExecuteNonQuery(); }

                                }
                            }
                            catch
                            {
                            }
                            //Console.WriteLine("close");
                            conn.Notification -= NotificationSupportHelper;
                            conn.Close();
                        }
                    }
                    catch
                    {
                        Thread.Sleep(1000);
                    }
            }
            catch (ThreadInterruptedException e)
            {
                //Console.WriteLine("newThread cannot go to sleep - interrupted by main thread.");
            }
        }

        void restartWatcher()
        {
            _state = false;
            _th.Interrupt();
            _th.Join(10000);
            _th = new Thread(Do);
            startListening();


        }

        private void WatchDog()
        {
            while (_state)
                try
                {
                    int i = 0;
                    while (_state && i < 100)
                    {
                        i++;
                        Thread.Sleep(100);
                    }
                    // чето wathdog завис передернем его
                    if (Math.Abs(DateTime.Now.ToOADate() - _lastWatch) * 24 * 60 * 60 > 60)
                    {
                        Console.WriteLine($"watcher завис - перезапуск потока {_watchSQL} {_cs}");
                        restartWatcher();
                    }
                    // давно не было сработок вотчера
                    if (Math.Abs(DateTime.Now.ToOADate() - _lastFire) * 24 * 60 * 60 > 300)
                    {
                        Console.WriteLine($"watcher не срабатывет - перезапуск потока {_watchSQL}");
                        _lastFire = DateTime.Now.ToOADate();
                        restartWatcher();
                        // посылаем сигнал обновить данные
                        if (onChange != null) onChange(this, "");

                    }

                }
                catch (Exception e)
                {
                    Console.WriteLine("ошибка watchdog " + e.Message);
                }
            
        }


        private void startListening()
        {
            _state = true;
            _th.Start();
        }

        public void stopListening()
        {
            _state = false;
            _th.Join();
            _thWD.Join();
        }

        private string buildConnectionString(string instr) {
            string result = "";
            string[] list = instr.Split(";");
            foreach (var elem in list)
                if (!elem.ToLower().Contains("commandtimeout")) result = result + elem + ";";
            return result + "; commandtimeout = 3;";
        }

        public PGDependency(string connectionString, string watchSQL)
        {
            _cs = buildConnectionString(connectionString);
            _watchSQL = watchSQL;
            _th = new Thread(Do);
            startListening();
            _thWD = new Thread(WatchDog);
            _thWD.Start();

        }

        ~PGDependency()
        {
            stopListening();
        }

    }
}
