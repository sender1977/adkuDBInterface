﻿using System.Collections.Generic;


namespace adkuDBInterface.Model
{
    public class SqlExecuteListResponse
    {
        // текст ошибки
        public string errorText { get; set; } = "";
        // список загруженных данных, в формате { поле: значение }
        public IList<IDictionary<int, SQLObject>> data = new List<IDictionary<int, SQLObject>>();
        public IDictionary<int, string> fieldNames = new Dictionary<int, string>();

        public IList<IDictionary<string, SQLObject>> getDataWithFields() {
            List<IDictionary<string, SQLObject>> result = new List<IDictionary<string, SQLObject>>();
            foreach (var row in data) {
                Dictionary<string, SQLObject> newRow = new Dictionary<string, SQLObject>();
                foreach (var key in row.Keys) {
                    newRow.Add(fieldNames[key], row[key]);
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
