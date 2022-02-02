using System.Collections.Generic;


namespace adkuDBInterface.Model
{
    public class SqlExecuteListResponse
    {
        // текст ошибки
        public string errorText { get; set; } = "";
        // список загруженных данных, в формате { поле: значение }
        public IList<IDictionary<int, SQLObject>> data = new List<IDictionary<int, SQLObject>>();
        public IDictionary<int, string> fieldNames = new Dictionary<int, string>();

        public IList<IDictionary<string, object>> getDataWithFields() {
            List<IDictionary<string, object>> result = new List<IDictionary<string, object>>();
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
        public void SetError(string error)
        {
            errorText = error;
            // сброс данных
            data = null;
        }
    }
}
