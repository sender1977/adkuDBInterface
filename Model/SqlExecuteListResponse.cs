using System.Collections.Generic;


namespace adkuDBInterface.Model
{
    public class SqlExecuteListResponse
    {
        // текст ошибки
        public string errorText { get; set; } = "";
        // список загруженных данных, в формате { поле: значение }
        public IList<IDictionary<int, SQLObject>> data = new List<IDictionary<int, SQLObject>>();

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
