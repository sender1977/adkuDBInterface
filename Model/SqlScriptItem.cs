using System;
using System.Collections.Generic;
using System.Text;

namespace adkuDBInterface.Model
{
    public class SQLScriptItem<T> where T : Enum
    {
        public T Sql { get; set; }
        public Dictionary<string, string> Params { get; set; }

    }

}
