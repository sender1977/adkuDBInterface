using System;
using PostgreSQLCopyHelper;

namespace adkuDBInterface.Model
{
    public interface IAdkuRecordBase : IComparable
    {
        object getField(int num);
        int getFieldCount();
    }

    public interface IAdkuRecord<T> : IAdkuRecordBase
    {
        PostgreSQLCopyHelper.PostgreSQLCopyHelper<T> getPGHelper(String TabName);
    }


}
