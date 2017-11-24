//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-07-14</createDate>
//<description>获取不良原因服务</description>
//---------------------------------------------------------------- 
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取不良原因服务
    /// </summary>
    [ServiceClass(typeof(IGetDefectiveReasonsService))]
    [Description("获取不良原因服务")]
    public class GetDefectiveReasonsService : ServiceComponent, IGetDefectiveReasonsService {
        /// <summary>
        /// 根据传入的不良原因编号，获取相应的不良原因信息
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="site_no">工厂</param>
        /// <param name="reason_code">理由码</param>
        /// <returns></returns>
        public Hashtable GetDefectiveReasons(string program_job_no, string status, string site_no, string reason_code) {
            try {
                //查询工厂的其他相关信息
                DependencyObjectCollection siteDetail = GetDefectiveReasons(reason_code, site_no); 
                //组织返回结果
                Hashtable result = new Hashtable { { "reason_list", siteDetail } };
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        /// 根据传入的不良原因编号，获取相应的不良原因信息
        /// </summary>
        /// <param name="reasonCode">入参理由码</param>
        /// <param name="siteNo"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetDefectiveReasons(string reasonCode,string siteNo) {
            //拼接where条件
            QueryConditionGroup conditionGroup = (OOQL.AuthFilter("DEFECTIVE_REASONS", "DEFECTIVE_REASONS"))
                                                 & (OOQL.CreateProperty("DEFECTIVE_REASONS.DEFECTIVE_TYPE").In(
                                                     OOQL.CreateConstants("1"),
                                                     OOQL.CreateConstants("2"),
                                                     OOQL.CreateConstants("6")))
                                                 & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));
            //IF 【入参理由码reason_code】不为空 THEN
            if (!Maths.IsEmpty(reasonCode)){
                conditionGroup &= (OOQL.CreateProperty("DEFECTIVE_REASONS.DEFECTIVE_REASONS_CODE") ==OOQL.CreateConstants(reasonCode));
            }
            //组合node
            QueryNode node = 
                OOQL.Select(
                            OOQL.CreateProperty("DEFECTIVE_REASONS.DEFECTIVE_REASONS_CODE", "reason_code"),
                            OOQL.CreateProperty("DEFECTIVE_REASONS.DESCRIPTION", "reason_code_name"))
                        .From("DEFECTIVE_REASONS", "DEFECTIVE_REASONS")
                        .InnerJoin("PLANT","PLANT")
                        .On(OOQL.CreateProperty("DEFECTIVE_REASONS.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                        .Where(conditionGroup);
            //查询数据并返回
            return GetService<IQueryService>().ExecuteDependencyObject(node); 
        }
    }
}
