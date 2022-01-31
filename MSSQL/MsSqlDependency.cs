using System.Threading;
using Microsoft.Data.SqlClient;
using adkuDBInterface.Model;

namespace adkuDBInterface.MSSQL
{

    public class MSSQLDependency
    {

        string _cs;
        string _watchSQL;
        SqlConnection _connection;
        bool _state = false;
        Thread _th;

        public event QueryChangeHandler onChange;
        private void startListening()
        {
            SqlDependency.Start(_cs);
            Watch();
        }

        private void Watch()
        {
            using (SqlCommand command = new SqlCommand(_watchSQL, _connection))
            {

                // Create a dependency and associate it with the SqlCommand.
                SqlDependency dependency = new SqlDependency(command);
                // Maintain the reference in a class member.

                // Subscribe to the SqlDependency event.
                dependency.OnChange += new
                   OnChangeEventHandler(OnDependencyChange);

                // Execute the command.
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    // Process the DataReader.
                }
            }
        }
        // Handler method
        private void OnDependencyChange(object sender, SqlNotificationEventArgs e)
        {
            if (onChange != null) onChange(sender, "");
            Watch();
        }

        public MSSQLDependency(string connectionString, string watchSQL, SqlConnection connection)
        {
            _cs = connectionString;
            _watchSQL = watchSQL;
            _connection = connection;
            startListening();
        }

    }
}
