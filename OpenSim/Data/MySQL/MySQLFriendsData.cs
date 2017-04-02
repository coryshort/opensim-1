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
using System.Collections;
using System.Collections.Generic;
using System.Data;
using OpenMetaverse;
using OpenSim.Framework;
using MySql.Data.MySqlClient;

namespace OpenSim.Data.MySQL
{
    public class MySqlFriendsData : MySQLGenericTableHandler<FriendsData>, IFriendsData
    {
        public MySqlFriendsData(string connectionString, string realm)
                : base(connectionString, realm, "FriendsStore")
        {
        }

        public bool Delete(UUID principalID, string friend)
        {
            return Delete(principalID.ToString(), friend);
        }

        public override bool Delete(string principalID, string friend)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.CommandText = "delete from " + m_Realm + 
                                  " where PrincipalID = '" + principalID + 
                                  "' and Friend = '" + friend + "'";
                ExecuteNonQuery(cmd);
            }

            return true;
        }

        public FriendsData[] GetFriends(UUID principalID)
        {
            return GetFriends(principalID.ToString());
        }

        public FriendsData[] GetFriends(string principalID)
        {
            using (MySqlCommand cmd = new MySqlCommand())
            {
                cmd.CommandText = "select a.*,case when b.Flags is null then -1 else b.Flags end as TheirFlags from " + m_Realm + 
                                  " as a left join " + m_Realm + 
                                  " as b on a.PrincipalID = b.Friend and a.Friend = b.PrincipalID where a.PrincipalID LIKE '" + principalID + "%'";
                return DoQuery(cmd);
            }
        }
    }
}