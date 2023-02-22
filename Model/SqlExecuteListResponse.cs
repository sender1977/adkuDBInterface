using System.Collections.Generic;


namespace adkuDBInterface.Model
{
    public class SqlExecuteListResponse
    {
        public string queryText { get; set; } = "";
        // текст ошибки
        public string errorText { get; set; } = "";
        // список загруженных данных, в формате { поле: значение }
        public IList<IDictionary<int, SQLObject>> data = new List<IDictionary<int, SQLObject>>();
        public IDictionary<int, string> fieldNames = new Dictionary<int, string>();

        public IList<IDictionary<string, object>> getDataWithFields() {
            List<IDictionary<string, object>> result = new List<IDictionary<string, object>>();
            if (data!=null)
            foreach (var row in data) {
                Dictionary<string, object> newRow = new Dictionary<string, object>();
                foreach (var key in row.Keys) {
                    newRow.Add(fieldNames[key], row[key].Value);
                }
                result.Add(newRow);
            }
            return result;
        }

        public override string ToString()
        {
            return "{\"ss\":\"bb\"}";
        }
        // установка ошибки
        public void SetError(string error, string query)
        {
            errorText = error;
            queryText = query;
            // сброс данных
            data = null;
        }
    }
}
