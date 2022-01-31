using System;
using System.Collections;
using System.Data;

namespace adkuDBInterface.Model
{
    public class QueueDataReader : IDataReader
    {

        private Queue cmdQueue;
        IAdkuRecord currRec;

        // Конструктор
        public QueueDataReader(Queue aCmdQueue)//, Type aRecType)
        {
            cmdQueue = aCmdQueue;
        }

        // Возвращаем значение, используя одну из функций преобразования и обработку исключения.
        // Это обезопасит нас от прерывания загрузки данных.

        public object GetValue(int i)
        {
            return currRec.getField(i);
        }

        // Чтение очередной строки.
        // Используем функции ограничения для того, чтобы еще на этапе чтения понять, что строка
        // вызовет исключения при передаче ее в SqlBulkCopy, поэтому мы пропускаем некорректные строки.

        public bool Read()
        {
            //if (cmdQueue.Count==0||cnt<=0) return false;
            if (cmdQueue.Count == 0) return false;
            //cnt--;
            currRec = (IAdkuRecord)cmdQueue.Dequeue();
            return true;// Read();
        }

        // Возвращем число столбцов в csv файле.
        // Нам заранее известно, что 4, поэтому не будем усложнять код.

        public int FieldCount
        {
            get
            {
                int res = 0;
                foreach (IAdkuRecord rec in cmdQueue)
                {
                    res = rec.getFieldCount();
                    break;
                }
                return res;
            }
        }

        // Освобождаем ресурсы. Закрываем поток.

        protected virtual void Dispose(bool flag)
        {
            currRec = null;
            cmdQueue = null;
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        // ... множестве нереализованных методов IDataReader, которые здесь не нужны.

        void IDataReader.Close()
        {
            throw new NotImplementedException();
        }

        int IDataReader.Depth
        {
            get { throw new NotImplementedException(); }
        }

        DataTable IDataReader.GetSchemaTable()
        {
            throw new NotImplementedException();
        }

        bool IDataReader.IsClosed
        {
            get { throw new NotImplementedException(); }
        }

        bool IDataReader.NextResult()
        {
            throw new NotImplementedException();
        }


        int IDataReader.RecordsAffected
        {
            get { throw new NotImplementedException(); }
        }





        bool IDataRecord.GetBoolean(int i)
        {
            throw new NotImplementedException();
        }

        byte IDataRecord.GetByte(int i)
        {
            throw new NotImplementedException();
        }

        long IDataRecord.GetBytes(int i, long fieldOffset, byte[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        char IDataRecord.GetChar(int i)
        {
            throw new NotImplementedException();
        }

        long IDataRecord.GetChars(int i, long fieldoffset, char[] buffer, int bufferoffset, int length)
        {
            throw new NotImplementedException();
        }

        IDataReader IDataRecord.GetData(int i)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetDataTypeName(int i)
        {
            throw new NotImplementedException();
        }

        DateTime IDataRecord.GetDateTime(int i)
        {
            throw new NotImplementedException();
        }

        decimal IDataRecord.GetDecimal(int i)
        {
            throw new NotImplementedException();
        }

        double IDataRecord.GetDouble(int i)
        {
            throw new NotImplementedException();
        }

        Type IDataRecord.GetFieldType(int i)
        {
            throw new NotImplementedException();
        }

        float IDataRecord.GetFloat(int i)
        {
            throw new NotImplementedException();
        }

        Guid IDataRecord.GetGuid(int i)
        {
            throw new NotImplementedException();
        }

        short IDataRecord.GetInt16(int i)
        {
            throw new NotImplementedException();
        }

        int IDataRecord.GetInt32(int i)
        {
            throw new NotImplementedException();
        }

        long IDataRecord.GetInt64(int i)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetName(int i)
        {
            throw new NotImplementedException();
        }

        int IDataRecord.GetOrdinal(string name)
        {
            throw new NotImplementedException();
        }

        string IDataRecord.GetString(int i)
        {
            throw new NotImplementedException();
        }

        int IDataRecord.GetValues(object[] values)
        {
            throw new NotImplementedException();
        }

        bool IDataRecord.IsDBNull(int i)
        {
            throw new NotImplementedException();
        }

        object IDataRecord.this[string name]
        {
            get { throw new NotImplementedException(); }
        }

        object IDataRecord.this[int i]
        {
            get { throw new NotImplementedException(); }
        }
    }
}
