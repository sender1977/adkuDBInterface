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

        private SortedList<string, PGDependency> _pgDependencies = new SortedList<string, PGDependency>();
        private SortedList<string, MSSQLDependency> _msDependencies = new SortedList<string, MSSQLDependency>();

        public ConnectionTypes ConnectionType { get => getConnectionType(); }

        public DBConnection(string connectionString, SortedList<T, SqlString> sqlConst)
        {
            _sqlConst = sqlConst;
            _connectionString = connectionString;
            _connType = getConnectionType();
        }
        public DBConnection(string connectionString, ConnectionTypes connectionType, SortedList<T, SqlString> sqlConst)
        {
            _sqlConst = sqlConst;
            _connectionString = connectionString;
            _connType = connectionType;

        }

        ~DBConnection()
        {


        }

        private string buildMSSQLConnectionString()
        {
            //user id = sa; data source = localhost; persist security info = True; initial catalog = npz1; password = 1; trust server certificate = True; Connection type = MSSQL
            StringBuilder result = new StringBuilder();
            var paramList = parseConnectionString();
            if (paramList.ContainsKey("server")) result.Append($@"data source = {paramList["server"]};");
            if (paramList.ContainsKey("data source")) result.Append($@"data source = {paramList["data source"]};");
            if (paramList.ContainsKey("database")) result.Append($@"initial catalog = {paramList["database"]};");
            if (paramList.ContainsKey("initial catalog")) result.Append($@"initial catalog = {paramList["initial catalog"]};");
            if (paramList.ContainsKey("user")) result.Append($@"user id = {paramList["user"]};");
            if (paramList.ContainsKey("user id")) result.Append($@"user id = {paramList["user id"]};");
            if (paramList.ContainsKey("password")) result.Append($@"password = {paramList["password"]};");
            result.Append($@"persist security info = True;");
            result.Append($@"trust server certificate = True;");
            return result.ToString();
        }

        private string buildPGConnectionString()
        {
            //Server = localhost; User Id = postgres; Database = ADKU; Port = 5432; Password = 1; SSLMode = Prefer; Search Path = web; Connection type = PG
            StringBuilder result = new StringBuilder();
            var paramList = parseConnectionString();
            if (paramList.ContainsKey("server")) result.Append($@"Server = {paramList["server"]};");
            if (paramList.ContainsKey("host")) result.Append($@"Server = {paramList["host"]};");
            if (paramList.ContainsKey("database")) result.Append($@"Database = {paramList["database"]};");
            if (paramList.ContainsKey("user")) result.Append($@"User Id = {paramList["user"]};");
            if (paramList.ContainsKey("user id")) result.Append($@"User Id = {paramList["user id"]};");
            if (paramList.ContainsKey("username")) result.Append($@"User Id = {paramList["username"]};");
            if (paramList.ContainsKey("password")) result.Append($@"Password = {paramList["password"]};");
            if (paramList.ContainsKey("search path")) result.Append($@"Search Path = {paramList["search path"]};");
            if (paramList.ContainsKey("port")) result.Append($@"Port = {paramList["port"]};"); else result.Append($@"Port = 5432;");
            result.Append($@"SSLMode = Prefer;");
            return result.ToString();
        }


        private SortedList<string, string> parseConnectionString()
        {
            SortedList<string, string> result = new SortedList<string, string>();
            try
            {
                foreach (string param in _connectionString.Split(';'))
                {
                    string[] arr = param.Split('=');
                    if (arr.Length > 1 && !result.ContainsKey(arr[0].Trim().ToLower())) result.Add(arr[0].Trim().ToLower(), arr[1].Trim());
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
            if (conParams.ContainsKey("connection type") && conParams["connection type"].ToLower() == "mssql" || conParams.ContainsKey("data source")) return ConnectionTypes.MSSQL;
            else if (conParams.ContainsKey("connection type") && conParams["connection type"].ToLower() == "pg" || conParams.ContainsKey("server") && conParams.ContainsKey("port")) return ConnectionTypes.PG;
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


        private string RetrievePartFromScriptByServerType(string text)
        {
            foreach (string script in text.Split("--@@"))
            {
                if (_connType == ConnectionTypes.MSSQL && script.ToUpper().StartsWith("--MSSQL")) return script;
                else if (_connType == ConnectionTypes.PG && script.ToUpper().StartsWith("--PG")) return script;
            }
            return text;
        }
        private async Task<string> ReadScript(string fileName)
        {
            string text = await System.IO.File.ReadAllTextAsync(fileName);
            return RetrievePartFromScriptByServerType(text);
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
            catch (Exception e)
            {
                var errRes = new SqlExecuteListResponse();
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

        private async Task<SqlExecuteListResponse> pgExecuteAndGetList(string query)
        {
            using (NpgsqlConnection _pgConn = new NpgsqlConnection(buildPGConnectionString()))
            {            //private SqlConnection _msConn = null;
                         //private NpgsqlConnection _pgConn = null;

                SqlExecuteListResponse response = new SqlExecuteListResponse();
                NpgsqlDataReader reader = null;
                try
                {

                    try
                    {
                        await _pgConn.OpenAsync();

                        var cmd = new NpgsqlCommand(query, _pgConn);
                        reader = await cmd.ExecuteReaderAsync(CommandBehavior.CloseConnection);

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
                        response.SetError(ex.Message);
                    }
                }
                finally
                {
                    if (reader != null) reader.Close();
                    _pgConn.Close();
                }
                // }

                return response;
            }
        }


        private async Task<SqlExecuteListResponse> msExecuteAndGetList(string query)
        {
            using (var _msConn = new SqlConnection(buildMSSQLConnectionString()))
            {
                SqlExecuteListResponse response = new SqlExecuteListResponse();
                //cnn.Open();

                SqlDataReader reader = null;

                try
                {
                    try
                    {
                        await _msConn.OpenAsync();
                        SqlCommand cmd = new SqlCommand();
                        cmd.Connection = _msConn;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = query;
                        reader = await cmd.ExecuteReaderAsync();

                        Dictionary<string, int> fields = new Dictionary<string, int>();
                        while (reader.Read())
                        {
                            // результут записываем в виде списка объектов { поле: значение }
                            // поле - название колонки, анонимная колонка получает название field + номер колонки
                            IDictionary<int, SQLObject> readerItem = new Dictionary<int, SQLObject>();
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
                        response.SetError(ex.Message);
                    }
                }
                finally
                {
                    if (reader != null) reader.Close();
                    _msConn.Close();
                }
                //}

                return response;
            }
        }

        private async Task<SqlExecuteListResponse> pgExecute(string query)
        {
            using (var _pgConn = new NpgsqlConnection(buildPGConnectionString()))
            {
                try
                {
                    SqlExecuteListResponse response = new SqlExecuteListResponse();


                    try
                    {
                        await _pgConn.OpenAsync();
                        var cmd = new NpgsqlCommand(query, _pgConn);
                        await cmd.ExecuteNonQueryAsync();

                    }
                    catch (Exception ex)
                    {
                        response.SetError(ex.Message);
                    }

                    return response;
                }
                finally
                {
                    _pgConn.Close();
                }
            }
        }

        private async Task<SqlExecuteListResponse> msExecute(string query)
        {
            using (var _msConn = new SqlConnection(buildMSSQLConnectionString()))
            {
                try
                {
                    SqlExecuteListResponse response = new SqlExecuteListResponse();


                    try
                    {
                        await _msConn.OpenAsync();
                        SqlCommand cmd = new SqlCommand();
                        cmd.Connection = _msConn;
                        cmd.CommandType = CommandType.Text;
                        cmd.CommandText = query;

                        await cmd.ExecuteNonQueryAsync();

                    }
                    catch (Exception ex)
                    {
                        response.SetError(ex.Message);
                    }

                    return response;
                }
                finally
                {
                    _msConn.Close();
                }
            }
        }

        private async Task<String> pgBulkSave(Queue q, string tab)
        {
            using (var _pgConn = new NpgsqlConnection(buildPGConnectionString()))
            {
                try
                {
                    try
                    {
                        await _pgConn.OpenAsync();
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
                        return e.Message;
                    }
                }
                finally
                {
                    _pgConn.Close();
                }
            }
        }

        private async Task<String> msBulkSave(Queue q, string tab)
        {

            if (!String.IsNullOrEmpty(tab))
                try
                {
                    IDataReader reader = new QueueDataReader(q);

                    // Создаем объект загрузчика SqlBulkCopy, указываем таблицу назначения и загружаем.
                    using (var loader = new SqlBulkCopy(buildMSSQLConnectionString(), SqlBulkCopyOptions.Default))
                    {
                        loader.BulkCopyTimeout = 10000;
                        loader.DestinationTableName = tab;
                        await loader.WriteToServerAsync(reader);
                    }
                }
                catch (Exception e)
                {
                    return e.Message;
                }
            return "";

        }

        private void pgWatch(string query, QueryChangeHandler onChange)
        {
            if (!_pgDependencies.ContainsKey(query))
            {
                PGDependency dep = new PGDependency(buildPGConnectionString(), query);
                _pgDependencies.Add(query, dep);
            }
            _pgDependencies[query].onChange += onChange;

        }

        private void msWatch(string query, QueryChangeHandler onChange)
        {

            if (!_msDependencies.ContainsKey(query))
            {
                MSSQLDependency dep = new MSSQLDependency(buildMSSQLConnectionString(), query, new SqlConnection(buildMSSQLConnectionString()));
                _msDependencies.Add(query, dep);
            }
            _msDependencies[query].onChange += onChange;

        }

    }

}
