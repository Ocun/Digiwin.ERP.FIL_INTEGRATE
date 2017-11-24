//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-11-17</createDate>
//<description>获取使用者信息实现</description>
//----------------------------------------------------------------
//20161222 modi by liwei1 for P001-161215001
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [ServiceClass(typeof(IGetUserService))]
    [Description("获取使用者信息实现")]
    public class GetUserService : ServiceComponent, IGetUserService {

        #region IGetUserService 成员

        /// <summary>
        /// 根据传入的账号，获取相应的用户信息
        /// </summary>
        /// <param name="account">账号</param>
        /// <returns></returns>
        public Hashtable GetUser(string account) {//20161222 add by liwei1 for P001-161215001
            //public Hashtable GetUser(string account, string site_no) {//20161222 mark by liwei1 for P001-161215001
            try {
                if (Maths.IsEmpty(account)) {
                    var infoCodeSer = GetService<IInfoEncodeContainer>(); //信息编码服务
                    throw new BusinessRuleException(infoCodeSer.GetMessage("A111201", "account"));//‘入参【account】未传值’
                }
                //获取用户信息
                QueryNode queryNode = GetenterpriseSite(account);
                DependencyObjectCollection userInfomation = GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组织返回结果
                Hashtable result = new Hashtable{{"enterprise_site", userInfomation}};
                return result;
            } catch (Exception) {
                throw;
            }
        }

        #endregion
        /// <summary>
        /// 获取用户信息
        /// </summary>
        /// <param name="account">账号</param>
        /// <returns></returns>
        private QueryNode GetenterpriseSite(string account) {
            return OOQL.Select(
                                   OOQL.CreateConstants(0, GeneralDBType.Int32, "code"),
                                   OOQL.CreateConstants("", GeneralDBType.String, "sql_code"),
                                   OOQL.CreateConstants("", GeneralDBType.String, "description"),
                                   OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                   OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                   OOQL.CreateProperty("USER.LOGONNAME", "account"),
                                   OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_lang"),
                                   OOQL.CreateConstants("", GeneralDBType.String, "site_lang"))
                               .From("USER", "USER")
                               .InnerJoin("USER.USER_ORG", "USER_ORG")
                               .On((OOQL.CreateProperty("USER_ORG.USER_ID") == OOQL.CreateProperty("USER.USER_ID")))
                               .InnerJoin("PLANT", "PLANT")
                               .On((OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("USER_ORG.ORG.ROid")))
                               .Where((OOQL.CreateProperty("USER.LOGONNAME") == OOQL.CreateConstants(account))
                                   & (OOQL.CreateProperty("USER_ORG.ORG.RTK") == OOQL.CreateConstants("PLANT")));

        }

    }
}
