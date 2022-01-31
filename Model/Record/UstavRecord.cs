using System;
using PostgreSQLCopyHelper;

namespace adkuDBInterface.Model
{
    public class UstavRecord : IAdkuRecord
    {
        public int TypeObj;
        public int IdObj;
        public int IdZap;
        public Double DValue;
        public string SValue;
        public DateTime ChTime;
        public DateTime MTime;

        public Type getType() => this.GetType();
        public PostgreSQLCopyHelper<UstavRecord> getPGHelper(String TabName) =>
            new PostgreSQLCopyHelper<UstavRecord>(TabName.Contains("##") ? "temp" : "public", TabName.Replace("##", ""))
         .MapInteger("typeobj", x => x.TypeObj)
         .MapInteger("idobj", x => x.IdObj)
         .MapInteger("idzap", x => x.IdZap)
         .MapDouble("val", x => x.DValue)
         .MapVarchar("valper", x => x.SValue)
         .MapTimeStamp("timeizm", x => x.ChTime)
         .MapTimeStamp("timeval", x => x.ChTime);

        public object getField(int num)
        {
            if (num == 0) return TypeObj;
            else if (num == 1) return IdObj;
            else if (num == 2) return IdZap;
            else if (num == 3) return DValue;
            else if (num == 4) return SValue;
            else if (num == 5) return MTime;
            else if (num == 6) return ChTime;
            else return null;
        }
        public int getFieldCount()
        {
            return 7;
        }

        public UstavRecord(int aTypeObj, int aIdObj, int aIdZap, Double aDValue, string aSValue)
        {
            TypeObj = aTypeObj;
            IdObj = aIdObj;
            IdZap = aIdZap;
            DValue = aDValue;
            SValue = aSValue;
            ChTime = DateTime.Now; //aChTime;
            MTime = DateTime.Now;
            //ChTime = aChTime;
            //MTime = aMTime;
        }

        public UstavRecord(int aTypeObj, int aIdObj, int aIdZap, Double aDValue, string aSValue, DateTime aChTime)
        {
            TypeObj = aTypeObj;
            IdObj = aIdObj;
            IdZap = aIdZap;
            DValue = aDValue;
            SValue = aSValue;
            ChTime = aChTime;
            MTime = DateTime.Now;
        }

        public UstavRecord(int aTypeObj, int aIdObj, int aIdZap, Double aDValue, string aSValue, DateTime aChTime, DateTime aMTime)
        {
            TypeObj = aTypeObj;
            IdObj = aIdObj;
            IdZap = aIdZap;
            DValue = aDValue;
            SValue = aSValue;
            ChTime = aChTime;
            MTime = aMTime;
        }


        public int CompareTo(object obj)
        {
            UstavRecord ct = (UstavRecord)obj;
            int res = this.TypeObj.CompareTo(ct.TypeObj);
            if (res != 0) return res;
            res = this.IdObj.CompareTo(ct.IdObj);
            if (res != 0) return res;
            res = this.IdZap.CompareTo(ct.IdZap);
            if (res != 0) return res;
            res = this.MTime.CompareTo(ct.MTime);
            return res;
        }
    }
}
