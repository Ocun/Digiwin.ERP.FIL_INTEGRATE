//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>营运中心信息获取服务</description>
//---------------------------------------------------------------- 
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 营运中心信息获取服务
    /// </summary>
    [ServiceClass(typeof(IGetPlantService))]
    [Description("营运中心信息获取服务")]
    public class GetPlantService:ServiceComponent,IGetPlantService {
        /// <summary>
        /// 根据传入的工厂编号，查询工厂的其他相关信息
        /// </summary>
        /// <param name="site_no">门店编号</param>
        /// <param name="enterprise_no">公司别</param>
        /// <returns></returns>
        public Hashtable GetPlant(string site_no, string enterprise_no) {
            try {
                //查询工厂的其他相关信息
                QueryNode queryNode = GetSiteDetail(site_no);
                DependencyObjectCollection siteDetail = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组织返回结果
                Hashtable result = new Hashtable{{"site_detail", siteDetail}};
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        /// 根据传入的工厂编号，查询工厂的其他相关信息
        /// </summary>
        /// <param name="siteNo">门店编号</param>
        /// <returns></returns>
        private QueryNode GetSiteDetail(string siteNo){
            QueryNode node = null;
            if (!Maths.IsEmpty(siteNo)){
                node = OOQL.Select(
                                        OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                        OOQL.CreateProperty("PLANT.PLANT_NAME", "site_name"),
                                        OOQL.CreateProperty("PLANT.ADDRESS", "address"),
                                        OOQL.CreateProperty("PLANT.TELEPHONE", "telephone"))
                                    .From("PLANT", "PLANT")
                                    .Where(OOQL.AuthFilter("PLANT", "PLANT")
                                            & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)));
            }
            else{
                node = OOQL.Select(
                                    OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"),
                                    OOQL.CreateProperty("PLANT.PLANT_NAME", "site_name"),
                                    OOQL.CreateProperty("PLANT.ADDRESS", "address"),
                                    OOQL.CreateProperty("PLANT.TELEPHONE", "telephone"))
                                .From("PLANT", "PLANT")
                                .Where(OOQL.AuthFilter("PLANT", "PLANT"));
            }
            return node;
        }
    }
}
