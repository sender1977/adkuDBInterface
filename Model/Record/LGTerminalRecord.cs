using System;
using PostgreSQLCopyHelper;


namespace adkuDBInterface.Model
{
    public class LGTerminalRecord : IAdkuRecord
    {
        public int IdModem;
        public int IdContr;
        public DateTime ValueTime;
        public int TypeMsg;
        public string Data;
        public string AddData;

        public PostgreSQLCopyHelper<LGTerminalRecord> getPGHelper(String TabName)
        {
            var helper = new PostgreSQLCopyHelper<LGTerminalRecord>("history", TabName)
         .MapInteger("idmodem", x => x.IdModem)
         .MapInteger("idcontr", x => x.IdContr)
         .MapTimeStamp("valuedate", x => x.ValueTime)
         .MapInteger("typemsg", x => x.TypeMsg)
         .MapText("data", x => x.Data)
         .MapText("adddata", x => x.Data);
            return helper;
        }

        public object getField(int num)
        {
            if (num == 0) return IdModem;
            else if (num == 1) return IdContr;
            else if (num == 2) return ValueTime;
            else if (num == 3) return TypeMsg;
            else if (num == 4) return Data;
            else if (num == 5) return AddData;
            else return null;
        }
        public int getFieldCount()
        {
            return 6;
        }

        public LGTerminalRecord(int aIdModem, int aIdContr, DateTime aValueTime, int aTypeMsg, string aData, string aAddData)
        {
            IdModem = aIdModem;
            IdContr = aIdContr;
            ValueTime = aValueTime;
            Data = aData;
            AddData = aAddData;
            TypeMsg = aTypeMsg;
        }
        public void Assign(LGTerminalRecord src)
        {
            this.IdModem = src.IdModem;
            this.IdContr = src.IdContr;
            this.ValueTime = src.ValueTime;
            this.Data = src.Data;
            this.TypeMsg = src.TypeMsg;
            this.AddData = src.AddData;
        }

        public int CompareTo(object obj)
        {
            LGTerminalRecord ct = (LGTerminalRecord)obj;
            int res = this.IdModem.CompareTo(ct.IdModem);
            if (res != 0) return res;
            res = this.IdContr.CompareTo(ct.IdContr);
            if (res != 0) return res;
            return this.ValueTime.CompareTo(ct.ValueTime);
        }
    }
}
