//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-15</createDate>
//<description>料件信息获取服务</description>
//---------------------------------------------------------------- 
using System;
using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    [ServiceClass(typeof(IGetItemInfoService))]
    [Description("料件信息获取服务")]
    public class GetItemInfoService : ServiceComponent, IGetItemInfoService {
        /// <summary>
        /// 根据传入的品号，查询品号的其他相关信息
        /// </summary>
        /// <param name="item_no">料件编号</param>
        /// <returns></returns>
        public Hashtable GetItemInfo(string item_no) {
            try {
                //查询品号的其他相关信息
                QueryNode queryNode = GetSiteDetail(item_no);
                DependencyObjectCollection siteDetail = this.GetService<IQueryService>().ExecuteDependencyObject(queryNode);

                //组合返回结果
                Hashtable result = new Hashtable();
                result.Add("site_detail", siteDetail);
                return result;
            } catch (Exception) {
                throw;
            }
        }

        /// <summary>
        ///查询品号的其他相关信息
        /// </summary>
        /// <param name="item_no">料件编号</param>
        private QueryNode GetSiteDetail(string item_no) {
            return OOQL.Select(
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(string.Empty), "unit_no"))
                                .From("ITEM", "ITEM")
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .Where((OOQL.AuthFilter("ITEM", "ITEM"))
                                        & (OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateConstants(item_no)));
        }

    }
}
