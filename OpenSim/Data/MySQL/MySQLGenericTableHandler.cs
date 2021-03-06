/*
 * Copyright (c) Contributors, http://opensimulator.org/
 * See CONTRIBUTORS.TXT for a full list of copyright holders.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the OpenSimulator Project nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE DEVELOPERS ``AS IS'' AND ANY
 * EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using log4net;
using MySql.Data.MySqlClient;
using OpenMetaverse;
using OpenSim.Framework;
using OpenSim.Region.Framework.Interfaces;

namespace OpenSim.Data.MySQL
{
    public class MySQLGenericTableHandler<T> : MySqlFramework where T: class, new()
    {
//        private static readonly ILog m_log = LogManager.GetLogger(MethodBase.GetCurrentMethod().DeclaringType);

        protected Dictionary<string, FieldInfo> m_Fields =
                new Dictionary<string, FieldInfo>();

        // protected List<string> m_ColumnNames = null;
        protected string[] m_ColumnNames = null;
        protected int Length_m_ColumnNames = 0;
        protected string m_Realm;
        protected FieldInfo m_DataField = null;

        protected virtual Assembly Assembly
        {
            get { return GetType().Assembly; }
        }

        public MySQLGenericTableHandler(string connectionString,
                string realm, string storeName) : base(connectionString)
        {
            m_Realm = realm;
            m_connectionString = connectionString;

            if (storeName != String.Empty)
            {
                using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
                {
                    dbcon.Open();
                    Migration m = new Migration(dbcon, Assembly, storeName);
                    m.Update();
                }
            }

            Type t = typeof(T);
            FieldInfo[] fields = t.GetFields(BindingFlags.Public |
                                             BindingFlags.Instance |
                                             BindingFlags.DeclaredOnly);

            if (fields.Length == 0)
                return;

            for(int i=0; i<fields.Length; i++)
            {
                if (fields[i].Name != "Data")
                    m_Fields[fields[i].Name] = fields[i];
                else
                    m_DataField = fields[i];
            }
        }

        private void CheckColumnNames(IDataReader reader)
        {
            if (m_ColumnNames != null)
                return;

            DataTable schemaTable = reader.GetSchemaTable();
            if (schemaTable.Rows.Count == 0)
                return;

            m_ColumnNames = new string[schemaTable.Rows.Count];

            // Yes we need to use Length_m_ColumnNames here.
            // m_ColumnNames may end up being longer than the list of non-key column names.
            Length_m_ColumnNames = 0;
            foreach (DataRow row in schemaTable.Rows)
            {
                if (row["ColumnName"] != null &&
                        (!m_Fields.ContainsKey(row["ColumnName"].ToString())))
                {
                    m_ColumnNames[Length_m_ColumnNames] = row["ColumnName"].ToString();
                    Length_m_ColumnNames++;
                }
            }
        }

        public virtual T[] Get(string field, string key)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.Parameters.AddWithValue(field, key);
                cmd.CommandText = "select * from " + m_Realm +
                                  " where `" + field + "` = ?" + field;

                return DoQuery(cmd);
            }
        }

        public virtual T[] Get(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length)
                return new T[0];

            using (MySqlCommand cmd = new MySqlCommand())
            {
                string[] terms = new string[fields.Length];

                for (int i = 0 ; i < fields.Length ; i++)
                {
                    cmd.Parameters.AddWithValue(fields[i], keys[i]);
                    terms[i] = "`" + fields[i] + "` = ?" + fields[i];
                }

                cmd.CommandText = "select * from " + m_Realm + 
                                  " where " + String.Join(" and ", terms);

                return DoQuery(cmd);
            }
        }

        protected T[] DoQuery(MySqlCommand cmd)
        {
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                cmd.Connection = dbcon;

                using (IDataReader reader = cmd.ExecuteReader())
                {
                    if (reader == null)
                        return new T[0];

                    CheckColumnNames(reader);

                    List<T> result = new List<T>();

                    while (reader.Read())
                    {
                        T row = new T();

                        foreach (string name in m_Fields.Keys)
                        {
                            if (reader[name] is DBNull)
                            {
                                continue;
                            }
                            if (m_Fields[name].FieldType == typeof(bool))
                            {
                                int v = Convert.ToInt32(reader[name]);
                                m_Fields[name].SetValue(row, v != 0 ? true : false);
                            }
                            else if (m_Fields[name].FieldType == typeof(UUID))
                            {
                                m_Fields[name].SetValue(row, DBGuid.FromDB(reader[name]));
                            }
                            else if (m_Fields[name].FieldType == typeof(int))
                            {
                                int v = Convert.ToInt32(reader[name]);
                                m_Fields[name].SetValue(row, v);
                            }
                            else if (m_Fields[name].FieldType == typeof(uint))
                            {
                                uint v = Convert.ToUInt32(reader[name]);
                                m_Fields[name].SetValue(row, v);
                            }
                            else
                            {
                                m_Fields[name].SetValue(row, reader[name]);
                            }
                        }

                        if (m_DataField != null)
                        {
                            Dictionary<string, string> data =
                                new Dictionary<string, string>();

                            // Yes we need to use Length_m_ColumnNames here.
                            // m_ColumnNames may be longer than the list of column names.
                            for (int i=0; i<Length_m_ColumnNames; i++)
                            {
                                data[m_ColumnNames[i]] = reader[m_ColumnNames[i]].ToString();
                                if (data[m_ColumnNames[i]] == null)
                                    data[m_ColumnNames[i]] = String.Empty;
                            }
                            m_DataField.SetValue(row, data);
                        }
                        result.Add(row);
                    }
                    return result.ToArray();
                }
            }
            // should never end up here
            return new T[0];
        }

        public virtual T[] Get(string where)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.CommandText = "select * from " + m_Realm + " where " + where;
                return DoQuery(cmd);
            }
        }

        public virtual bool Store(T row)
        {
//            m_log.DebugFormat("[MYSQL GENERIC TABLE HANDLER]: Store(T row) invoked");

            using (MySqlCommand cmd = new MySqlCommand())
            {
                List<String> names = new List<String>();
                List<String> values = new List<String>();

                foreach (FieldInfo fi in m_Fields.Values)
                {
                    names.Add(fi.Name);
                    values.Add("?" + fi.Name);

                    // Temporarily return more information about what field is unexpectedly null for
                    // http://opensimulator.org/mantis/view.php?id=5403.  This might be due to a bug in the
                    // InventoryTransferModule or we may be required to substitute a DBNull here.
                    if (fi.GetValue(row) == null)
                        throw new NullReferenceException(
                            string.Format(
                                "[MYSQL GENERIC TABLE HANDLER]: Trying to store field {0} for {1} which is unexpectedly null",
                                fi.Name, row));

                    cmd.Parameters.AddWithValue(fi.Name, fi.GetValue(row).ToString());
                }

                if (m_DataField != null)
                {
                    Dictionary<string, string> data =
                        (Dictionary<string, string>)m_DataField.GetValue(row);

                    foreach (KeyValuePair<string, string> kvp in data)
                    {
                        names.Add(kvp.Key);
                        values.Add("?" + kvp.Key);
                        cmd.Parameters.AddWithValue("?" + kvp.Key, kvp.Value);
                    }
                }

                cmd.CommandText = "replace into " + m_Realm + 
                                  " (`" + String.Join("`,`", names.ToArray()) + 
                                  "`) values (" + String.Join(",", values.ToArray()) + ")";

                return (ExecuteNonQuery(cmd) > 0);
            }
        }

        public virtual bool Delete(string field, string key)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.Parameters.AddWithValue(field, key);
                cmd.CommandText = "delete from " + m_Realm + " where `" + field + "` = ?" + field;

                return (ExecuteNonQuery(cmd) > 0);
            }
        }

        public virtual bool Delete(string[] fields, string[] keys)
        {
//            m_log.DebugFormat(
//                "[MYSQL GENERIC TABLE HANDLER]: Delete(string[] fields, string[] keys) invoked with {0}:{1}",
//                string.Join(",", fields), string.Join(",", keys));

            if (fields.Length != keys.Length)
                return false;

            using (MySqlCommand cmd = new MySqlCommand())
            {
                string[] terms = new string[fields.Length]; 

                for (int i = 0 ; i < fields.Length ; i++)
                {
                    cmd.Parameters.AddWithValue(fields[i], keys[i]);
                    terms[i] = "`" + fields[i] + "` = ?" + fields[i];
                }

                cmd.CommandText = "delete from " + m_Realm + " where " + String.Join(" and ", terms);

                return (ExecuteNonQuery(cmd) > 0);
            }
        }

        public long GetCount(string field, string key)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.Parameters.AddWithValue(field, key);
                cmd.CommandText = "select count(*) from " + m_Realm + " where `" + field + "` = ?" + field;

                Object result = DoQueryScalar(cmd);

                return Convert.ToInt64(result);
            }
        }

        public long GetCount(string[] fields, string[] keys)
        {
            if (fields.Length != keys.Length)
                return 0;

            using (MySqlCommand cmd = new MySqlCommand())
            {
                string[] terms = new string[fields.Length];

                for (int i = 0; i < fields.Length; i++)
                {
                    cmd.Parameters.AddWithValue(fields[i], keys[i]);
                    terms[i] = "`" + fields[i] + "` = ?" + fields[i];
                }

                cmd.CommandText = "select count(*) from " + m_Realm + " where " + String.Join(" and ", terms);

                Object result = DoQueryScalar(cmd);

                return Convert.ToInt64(result);
            }
        }

        public long GetCount(string where)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.CommandText = "select count(*) from " + m_Realm + " where " + where;

                object result = DoQueryScalar(cmd);

                return Convert.ToInt64(result);
            }
        }

        public object DoQueryScalar(MySqlCommand cmd)
        {
            using (MySqlConnection dbcon = new MySqlConnection(m_connectionString))
            {
                dbcon.Open();
                cmd.Connection = dbcon;

                return cmd.ExecuteScalar();
            }
        }

    }
}
