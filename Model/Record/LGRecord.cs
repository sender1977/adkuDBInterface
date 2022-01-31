using System;
using PostgreSQLCopyHelper;


namespace adkuDBInterface.Model
{
    public class LGRecord : IAdkuRecord
    {
        public int TypeObj;
        public int IdPar;
        public int IdObj;
        public Double DValue;
        public DateTime ValueTime;
        public int Period;
        public DateTime WriteDate = DateTime.UtcNow;
        public int UserId = 1;
        public int Del = 0;
        public int Flag;
        public int Quality = 192;
        public bool NoFlat;

        public PostgreSQLCopyHelper<LGRecord> getPGHelper(String TabName)
        {
            var helper = new PostgreSQLCopyHelper<LGRecord>(TabName.Contains("##") ? "temp" : "history", TabName.Replace("##", ""))
         .MapInteger("typeobj", x => x.TypeObj)
         .MapInteger("idpar", x => x.IdPar)
         .MapInteger("idobj", x => x.IdObj)
         .MapDouble("dvalue", x => x.DValue)
         .MapTimeStamp("valuedate", x => x.ValueTime)
         .MapInteger("period", x => x.Period)
         .MapInteger("userid", x => x.UserId)
         .MapBoolean("del", x => x.Del > 0);
            return helper;// (PostgreSQLCopyHelper<IAdkuRecord>)helper;
        }

        public object getField(int num)
        {
            if (num == 0) return TypeObj;
            else if (num == 2) return IdPar;
            else if (num == 1) return IdObj;
            else if (num == 3 && DValue != -32000) return DValue;
            else if (num == 3 && DValue == -32000) return DBNull.Value;
            else if (num == 4) return ValueTime;
            else if (num == 5) return Period;
            else if (num == 6) return WriteDate;
            else if (num == 7) return UserId;
            else if (num == 8) return Del;
            //else if (num == 9) return Quality;
            else return null;
        }
        public int getFieldCount()
        {
            return 9;
        }

        public LGRecord(int aTypeObj, int aIdPar, int aIdObj, Double aDValue, DateTime aValueTime, int aPeriod, int aFlag, int aQvt = 192)
        {
            TypeObj = aTypeObj;
            IdPar = aIdPar;
            IdObj = aIdObj;
            DValue = aDValue;
            ValueTime = aValueTime;
            Period = aPeriod;
            Flag = aFlag;
            NoFlat = false;
            Quality = aQvt;
        }
        public void Assign(LGRecord src)
        {
            this.TypeObj = src.TypeObj;
            this.IdPar = src.IdPar;
            this.IdObj = src.IdObj;
            this.DValue = src.DValue;
            this.ValueTime = src.ValueTime;
            this.Period = src.Period;
            this.Flag = src.Flag;
            this.NoFlat = src.NoFlat;
            this.Quality = src.Quality;
        }

        public int CompareTo(object obj)
        {
            LGRecord ct = (LGRecord)obj;
            int res = this.TypeObj.CompareTo(ct.TypeObj);
            if (res != 0) return res;
            res = this.IdObj.CompareTo(ct.IdObj);
            if (res != 0) return res;
            res = this.IdPar.CompareTo(ct.IdPar);
            if (res != 0) return res;
            return this.ValueTime.CompareTo(ct.ValueTime);
        }
    }
}
