using System;

namespace adkuDBInterface.Model
{
    public class SQLObject
    {
        private object _value;
        public SQLObject(object value)
        {
            _value = value;
        }
        public override string ToString()
        {
            if (_value == null) return ""; else return _value.ToString();
        }
        public bool IsDBNull() {
            return _value == null;
        }
        public DateTime GetDateTime()
        {
            if (_value != null)
            {
                Double dbl = 0;
                if (Double.TryParse(_value.ToString(), out dbl)) return DateTime.FromOADate(dbl);
                else
                return (DateTime)_value;
            }
            else throw new Exception("Bad DateTime");
        }

        
        public Int32 GetInt32()
        {
            if (_value != null)
            {
                int result = 0;
                if (!int.TryParse(this.ToString(), out result)) throw new Exception("Bad int");
                else return result;
            }
            else throw new Exception("Bad int");
        }
        public Double GetDouble()
        {
            if (_value != null)
            {
                Double result = 0;
                if (!Double.TryParse(this.ToString(), out result)) throw new Exception("Bad double");
                else return result;
            }
            else throw new Exception("Bad double");
        }

    }
}
