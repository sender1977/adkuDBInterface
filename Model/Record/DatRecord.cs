using System;
using PostgreSQLCopyHelper;

namespace adkuDBInterface.Model
{
    public class DatRecord : IAdkuRecord
    {
        public int IdDat;
        public Double DValue;
        public DateTime ValueTime;
        public int Quality;
        public Type getType() => this.GetType();
        public PostgreSQLCopyHelper<DatRecord> getPGHelper(String TabName) =>
            new PostgreSQLCopyHelper<DatRecord>(TabName.Contains("##") ? "temp" : "public", TabName.Replace("##", ""))
         .MapInteger("iddat", x => x.IdDat)
         .MapDouble("dvalue", x => x.DValue)
         .MapTimeStamp("valuetime", x => x.ValueTime)
         .MapInteger("qvt", x => x.Quality);

        public object getField(int num)
        {
            if (num == 0) return IdDat;
            else if (num == 1) return DValue;
            else if (num == 2) return ValueTime;
            else if (num == 3) return Quality;
            else return null;
        }
        public int getFieldCount()
        {
            return 4;
        }

        public DatRecord(int aIdDat, Double aDValue, DateTime aValueTime, int aQuality)
        {
            IdDat = aIdDat;
            DValue = aDValue;
            ValueTime = aValueTime;
            Quality = aQuality;
        }

        public int CompareTo(object obj)
        {
            DatRecord ct = (DatRecord)obj;
            return this.IdDat.CompareTo(ct.IdDat);
        }
    }

}
