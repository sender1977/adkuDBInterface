using System;
using PostgreSQLCopyHelper;

namespace adkuDBInterface.Model
{
    public class LGFlatRecord : IAdkuRecord
    {
        public int TypeObj;
        public int IdObj;
        public DateTime DT;
        public int Registr;
        public int Flag;
        public float[] Data;
        public Type getType() => this.GetType();
        public PostgreSQLCopyHelper<LGFlatRecord> getPGHelper(String TabName)
        {
            var helper = new PostgreSQLCopyHelper<LGFlatRecord>(TabName.Contains("##") ? "temp" : "history", TabName.Replace("##", ""))
         .MapInteger("typeobj", x => x.TypeObj)
         .MapInteger("idobj", x => x.IdObj)
         .MapSmallInt("registr", x => (short)x.Registr)
         .MapTimeStamp("valuedate", x => x.DT);
            for (int i = 0; i < Data.Length; i++)
            {
                int ix = i;
                helper.MapReal($"p{ix + 1}", x => x.Data[ix]);
            }

            return helper;
        }

        public LGFlatRecord(int aTypeObj, int aIdObj, DateTime aDT, int aRegistr, int aFlag = 3, int Size = 0)
        {
            this.TypeObj = aTypeObj;
            this.IdObj = aIdObj;
            this.DT = aDT;
            this.Registr = aRegistr;
            this.Flag = aFlag;
            this.Data = new float [Size];
            for (int i = 0; i < Size; i++) Data[i] = -32000;
        }


        public object getField(int num)
        {
            if (num == 0) return TypeObj;
            else if (num == 1) return IdObj;
            else if (num == 3) return DT;
            else if (num == 2) return Registr;
            else if (num >= 4 /*&& num < Const.ANLG_RECORD_FIELD_COUNT + 4 && Data[num - 4] != -1000*/) return Data[num - 4];
            else return null;
        }
        public int getFieldCount()
        {
            return this.Data.Length + 4;
        }

        public void Assign(LGFlatRecord src)
        {
            this.TypeObj = src.TypeObj;
            this.IdObj = src.IdObj;
            this.DT = src.DT;
            this.Registr = src.Registr;
            src.Data.CopyTo(this.Data, 0);
        }

        public int CompareTo(object obj)
        {
            LGFlatRecord ct = (LGFlatRecord)obj;
            int res = this.TypeObj.CompareTo(ct.TypeObj);
            if (res != 0) return res;
            res = this.IdObj.CompareTo(ct.IdObj);
            if (res != 0) return res;
            res = this.Registr.CompareTo(ct.Registr);
            if (res != 0) return res;
            return this.DT.CompareTo(ct.DT);
        }
    }
}
