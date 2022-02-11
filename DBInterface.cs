using System;
using System.Threading;
using System.Data;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using adkuDBInterface.Model;
using adkuDBInterface.PG;
using adkuDBInterface.MSSQL;

namespace adkuDBInterface
{

        //"user id=sa;data source=srv-tm-ias\\sql2014;persist security info=True;initial catalog=npz1;password=1"
        //"Server=srv-tm-ias;Port=5432;Database=ADKU;User Id=postgres;Password=1;"
        public class DBConnection<T> where T : Enum
        {
            private SortedList<T, SqlString> _sqlConst;
            private string _connectionString;
            private ConnectionTypes _connType = ConnectionTypes.MSSQL;
            private SqlConnection _msConn = null;
            private NpgsqlConnection _pgConn = null;
            private SortedList<string, PGDependency> _pgDependencies = new SortedList<string, PGDependency>();
            private SortedList<string, MSSQLDependency> _msDependencies = new SortedList<string, MSSQLDependency>();

            public ConnectionTypes ConnectionType { get => getConnectionType(); }

            public DBConnection(string connectionString, SortedList<T, SqlString> sqlConst)
            {
                _sqlConst = sqlConst;
                _connectionString = connectionString;
                _connType = getConnectionType();
                CreateConnection();
            }
            public DBConnection(string connectionString, ConnectionTypes connectionType, SortedList<T, SqlString> sqlConst)
            {
                _sqlConst = sqlConst;
                _connectionString = connectionString;
                _connType = connectionType;
                CreateConnection();
            }

            ~DBConnection()
            {
               // Close();
                _pgConn = null;
                _msConn = null;
            }


            private void CreateConnection()
            {
                try
                {
                    if (_connType == ConnectionTypes.MSSQL)
                    {
                        _msConn = new SqlConnection(_connectionString);
                        _msConn.Open();
                    }
                    else if (_connType == ConnectionTypes.PG)
                    {
                        _pgConn = new NpgsqlConnection(_connectionString);
                        _pgConn.Open();
                    }
                }
                catch (Exception e)
                {
                    _msConn = null;
                    _pgConn = null;
                    throw new Exception($"Не удалось создать соединение ({e.Message})");
                }
            }

            private SortedList<string, string> parseConnectionString()
            {
                SortedList<string, string> result = new SortedList<string, string>();
                try
                {
                    foreach (string param in _connectionString.ToLower().Split(';'))
                    {
                        string[] arr = param.Split('=');
                        if (arr.Length > 1 && !result.ContainsKey(arr[0])) result.Add(arr[0].Trim(), arr[1].Trim());
                    }
                }
                catch
                {

                }
                return result;
            }
            private ConnectionTypes getConnectionType()
            {
                SortedList<string, string> conParams = parseConnectionString();
                if (conParams.ContainsKey("data source")) return ConnectionTypes.MSSQL;
                else if (conParams.ContainsKey("server") && conParams.ContainsKey("port")) return ConnectionTypes.PG;
                else return ConnectionTypes.MSSQL;
            }
            private string prepareSql(string template, Dictionary<string, string> paramList)
            {
                string res = template;
                if (paramList != null)
                    foreach (var par in paramList.Keys)
                        res = res.Replace(par, paramList[par]);
                return res;
            }

            private string prepareSqlScript(List<SQLScriptItem<T>> script)
            {
                StringBuilder res = new StringBuilder();
                foreach (var row in script)
                {
                    var src = "";
                    if (_connType == ConnectionTypes.MSSQL) src = _sqlConst[row.Sql].sql_mssql;
                    else if (_connType == ConnectionTypes.PG) src = _sqlConst[row.Sql].sql_pg;

                    if (row.Params != null)
                    foreach (var par in row.Params.Keys)
                        src = src.Replace(par, row.Params[par]);
                    res.AppendLine(src);
                }
                return res.ToString();
            }

            private void Close()
            {
                if (_pgConn != null && _pgConn.State==ConnectionState.Open) _pgConn.Close();
                if (_msConn != null && _msConn.State==ConnectionState.Open) _msConn.Close();

            }
        private string RetrievePartFromScriptByServerType(string text) {
            foreach (string script in text.Split("--@@"))
            {
                if (_connType == ConnectionTypes.MSSQL && script.ToUpper().StartsWith("--MSSQL")) return script;
                else if (_connType == ConnectionTypes.PG && script.ToUpper().StartsWith("--PG")) return script;
            }
            return text;
        }
        private async Task<string> ReadScript(string fileName) {
            string text = await System.IO.File.ReadAllTextAsync(fileName);
            return RetrievePartFromScriptByServerType(text);
            /*foreach (string script in text.Split("--@@"))
            {
                if (_connType == ConnectionTypes.MSSQL && script.ToUpper().StartsWith("--MSSQL")) return script;
                else if (_connType == ConnectionTypes.PG && script.ToUpper().StartsWith("--PG")) return script;
            }
            return text;*/
        }


        public async Task<SqlExecuteListResponse> ExecuteAndGetList(T sql, Dictionary<string, string> paramList)
            {
                if (_connType == ConnectionTypes.MSSQL) return await msExecuteAndGetList(prepareSql(_sqlConst[sql].sql_mssql, paramList));
                else if (_connType == ConnectionTypes.PG) return await pgExecuteAndGetList(prepareSql(_sqlConst[sql].sql_pg, paramList));
                else return new SqlExecuteListResponse();
            }
        public async Task<SqlExecuteListResponse> ExecuteAndGetList(List<SQLScriptItem<T>> script)
        {
            if (_connType == ConnectionTypes.MSSQL) return await msExecuteAndGetList(prepareSqlScript(script));
            else if (_connType == ConnectionTypes.PG) return await pgExecuteAndGetList(prepareSqlScript(script));
            else return new SqlExecuteListResponse();
        }
        public async Task<SqlExecuteListResponse> ExecuteAndGetList(string fileName, Dictionary<string, string> paramList)
        {
            try
            {
                var script = await ReadScript(fileName);
                if (_connType == ConnectionTypes.MSSQL) return await msExecuteAndGetList(prepareSql(script, paramList));
                else if (_connType == ConnectionTypes.PG) return await pgExecuteAndGetList(prepareSql(script, paramList));
                else return new SqlExecuteListResponse();
            }
            catch (Exception e) {
                var errRes= new SqlExecuteListResponse();
                errRes.errorText = e.Message;
                return errRes;
            }
        }

        public async Task<SqlExecuteListResponse> ExecuteAndGetList(string sql)
        {
            var script = RetrievePartFromScriptByServerType(sql);
            if (_connType == ConnectionTypes.MSSQL) return await msExecuteAndGetList(script);
            else if (_connType == ConnectionTypes.PG) return await pgExecuteAndGetList(script);
            else return new SqlExecuteListResponse();
        }

        public async Task<SqlExecuteListResponse> Execute(T sql, Dictionary<string, string> paramList)
            {
                if (_connType == ConnectionTypes.MSSQL) return await msExecute(prepareSql(_sqlConst[sql].sql_mssql, paramList));
                else if (_connType == ConnectionTypes.PG) return await pgExecute(prepareSql(_sqlConst[sql].sql_pg, paramList));
                else return new SqlExecuteListResponse();
            }
            public async Task<SqlExecuteListResponse> Execute(List<SQLScriptItem<T>> script)
            {
                if (_connType == ConnectionTypes.MSSQL) return await msExecute(prepareSqlScript(script));
                else if (_connType == ConnectionTypes.PG) return await pgExecute(prepareSqlScript(script));
                else return new SqlExecuteListResponse();
            }

        public async Task<SqlExecuteListResponse> Execute(string fileName, Dictionary<string, string> paramList)
        {
            try
            {
                var script = await ReadScript(fileName);
                if (_connType == ConnectionTypes.MSSQL) return await msExecute(prepareSql(script, paramList));
                else if (_connType == ConnectionTypes.PG) return await pgExecute(prepareSql(script, paramList));
                else return new SqlExecuteListResponse();
            }
            catch (Exception e)
            {
                var errRes = new SqlExecuteListResponse();
                errRes.errorText = e.Message;
                return errRes;
            }
        }
        public async Task<SqlExecuteListResponse> Execute(string sql)
        {
            var script = RetrievePartFromScriptByServerType(sql);
            if (_connType == ConnectionTypes.MSSQL) return await msExecute(script);
            else if (_connType == ConnectionTypes.PG) return await pgExecute(script);
            else return new SqlExecuteListResponse();
        }

        public async Task<string> BulkSave(Queue q, string tab)
            {
                if (_connType == ConnectionTypes.MSSQL) return await msBulkSave(q, tab);
                else if (_connType == ConnectionTypes.PG) return await pgBulkSave(q, tab);

                return "";

            }

            public void Watch(T sql, Dictionary<string, string> paramList, QueryChangeHandler onChange)
            {
                if (_connType == ConnectionTypes.MSSQL) msWatch(prepareSql(_sqlConst[sql].sql_mssql, paramList), onChange);
                else if (_connType == ConnectionTypes.PG) pgWatch(prepareSql(_sqlConst[sql].sql_pg, paramList), onChange);
            }

            public async void Reopen()
            {
                if (_connType == ConnectionTypes.MSSQL)
                {
                    _msConn.Close();
                    await _msConn.OpenAsync();
                }
                else if (_connType == ConnectionTypes.PG)
                {
                    _pgConn.Close();
                    await _pgConn.OpenAsync();
                }
            }

            private async Task<SqlExecuteListResponse> pgExecuteAndGetList(string query)
            {
                SqlExecuteListResponse response = new SqlExecuteListResponse();
                //using (var conn = new NpgsqlConnection(_connectionString))
                //{
                NpgsqlDataReader reader = null;
                try
                {
                    try
                    {

                        //await conn.OpenAsync();
                        await using var cmd = new NpgsqlCommand(query, _pgConn);
                        reader = await cmd.ExecuteReaderAsync();

                        while (await reader.ReadAsync())
                        {
                            IDictionary<int, SQLObject> readerItem = new Dictionary<int, SQLObject>();
                            // результут записываем в виде списка объектов { поле: значение }
                            // поле - название колонки, анонимная колонка получает название field + номер колонки
                            //IDictionary<string, object> readerItem = new Dictionary<string, object>();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                            if (!response.fieldNames.ContainsKey(i))
                            {
                                string name = reader.GetName(i);
                                if (name == "") name = "field" + i.ToString();
                                while (response.fieldNames.Values.Contains(name)) name += "_";
                                response.fieldNames.Add(i, name);
                            }
                            readerItem.Add(i, new SQLObject(reader.IsDBNull(i) ? null : reader[i]));
                            }


                            response.data.Add(readerItem);
                        }
                    }
                    catch (Exception ex)
                    {
                        response.SetError(ex.ToString());
                    }
                }
                finally
                {
                    if (reader != null) reader.Close();
                }
                // }

                return response;
            }


            private async Task<SqlExecuteListResponse> msExecuteAndGetList(string query)
            {
                SqlExecuteListResponse response = new SqlExecuteListResponse();

                //using (SqlConnection cnn = new SqlConnection(_connectionString))
                //{
                SqlCommand cmd = new SqlCommand();
                cmd.Connection = _msConn;
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;
                //cnn.Open();

                SqlDataReader reader = await cmd.ExecuteReaderAsync();

                try
                {
                    try
                    {
                    Dictionary<string, int> fields = new Dictionary<string, int>();
                    while (reader.Read())
                    {
                        // результут записываем в виде списка объектов { поле: значение }
                        // поле - название колонки, анонимная колонка получает название field + номер колонки
                        IDictionary<int, SQLObject> readerItem = new Dictionary<int, SQLObject>();
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            if (!response.fieldNames.ContainsKey(i)) {
                                string name = reader.GetName(i);
                                if (name == "") name = "field" + i.ToString();
                                while (response.fieldNames.Values.Contains(name)) name += "_";
                                response.fieldNames.Add(i, name);
                            }
                                readerItem.Add(i, new SQLObject(reader.IsDBNull(i) ? null : reader[i]));
                            }
                            response.data.Add(readerItem);
                        }
                    }
                    catch (Exception ex)
                    {
                        response.SetError(ex.ToString());
                    }
                }
                finally
                {
                    if (reader != null) reader.Close();
                }
                //}

                return response;

            }

            private async Task<SqlExecuteListResponse> pgExecute(string query)
            {
                SqlExecuteListResponse response = new SqlExecuteListResponse();

                await using var cmd = new NpgsqlCommand(query, _pgConn);

                try
                {
                    await cmd.ExecuteNonQueryAsync();

                }
                catch (Exception ex)
                {
                    response.SetError(ex.ToString());
                }

                return response;

            }

            private async Task<SqlExecuteListResponse> msExecute(string query)
            {
                SqlExecuteListResponse response = new SqlExecuteListResponse();

                SqlCommand cmd = new SqlCommand();
                cmd.Connection = _msConn;
                cmd.CommandType = CommandType.Text;
                cmd.CommandText = query;

                try
                {
                    await cmd.ExecuteNonQueryAsync();

                }
                catch (Exception ex)
                {
                    response.SetError(ex.ToString());
                }

                return response;

            }

            private async Task<String> pgBulkSave(Queue q, string tab)
            {
                try
                {
                    string quotab = tab;// $"\"{tab}\"";
                    List<object> entities = new List<object>();
                    while (q.Count > 0) entities.Add(q.Dequeue());
                    if (entities.Count > 0)
                    {
                        var rec = entities.First();
                        if (rec is LGRecord)
                        {
                            var helper = ((LGRecord)rec).getPGHelper(quotab);
                            await helper.SaveAllAsync(_pgConn, entities.Cast<LGRecord>());
                        }
                        else if (rec is LGFlatRecord)
                        {
                            var helper = ((LGFlatRecord)rec).getPGHelper(quotab);
                            await helper.SaveAllAsync(_pgConn, entities.Cast<LGFlatRecord>());
                        }
                        else if (rec is DatRecord)
                        {
                            var helper = ((DatRecord)rec).getPGHelper(quotab);
                            await helper.SaveAllAsync(_pgConn, entities.Cast<DatRecord>());
                        }
                        else if (rec is UstavRecord)
                        {
                            var helper = ((UstavRecord)rec).getPGHelper(quotab);
                            await helper.SaveAllAsync(_pgConn, entities.Cast<UstavRecord>());
                        }

                    }
                    return "";
                }
                catch (Exception e)
                {
                    Reopen();
                    return e.Message;
                }
            }

            private async Task<String> msBulkSave(Queue q, string tab)
            {

                if (!String.IsNullOrEmpty(tab))
                    try
                    {
                        IDataReader reader = new QueueDataReader(q);

                        // Создаем объект загрузчика SqlBulkCopy, указываем таблицу назначения и загружаем.
                        using (var loader = new SqlBulkCopy(_connectionString, SqlBulkCopyOptions.Default))
                        {
                            loader.BulkCopyTimeout = 10000;
                            loader.DestinationTableName = tab;
                            await loader.WriteToServerAsync(reader);
                        }
                    }
                    catch (Exception e)
                    {
                        Reopen();
                        return e.Message;
                    }
                return "";

            }

            private void pgWatch(string query, QueryChangeHandler onChange)
            {
                if (!_pgDependencies.ContainsKey(query))
                {
                    PGDependency dep = new PGDependency(_connectionString, query);
                    _pgDependencies.Add(query, dep);
                }
                _pgDependencies[query].onChange += onChange;
                //return _pgDependencies[query];
            }

            private void msWatch(string query, QueryChangeHandler onChange)
            {
                if (!_msDependencies.ContainsKey(query))
                {
                    MSSQLDependency dep = new MSSQLDependency(_connectionString, query, _msConn);
                    _msDependencies.Add(query, dep);
                }
                _msDependencies[query].onChange += onChange;
            }

        }

}
