using System;

namespace adkuDBInterface.Model
{
    public interface IAdkuRecord : IComparable
    {
        object getField(int num);
        int getFieldCount();
    }


}
