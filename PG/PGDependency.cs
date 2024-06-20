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
        Thread _dth;
        public event QueryChangeHandler onChange;



        private void NotificationSupportHelper(Object sender, NpgsqlNotificationEventArgs e)
        {
            if (onChange != null && _watchSQL.ToLower().Contains(e.Payload.ToLower()))
            {
                // задержка при массовом обновлении чтобы не сыпать сообщениями попусту
                if (_dth != null) _dth.Interrupt();
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
                                //Console.WriteLine("wait");
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
                    Thread.Sleep(300);
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
        }

        public PGDependency(string connectionString, string watchSQL)
        {
            _cs = connectionString;
            _watchSQL = watchSQL;
            _th = new Thread(Do);
            startListening();

        }

        ~PGDependency()
        {
            stopListening();
        }

    }
}
