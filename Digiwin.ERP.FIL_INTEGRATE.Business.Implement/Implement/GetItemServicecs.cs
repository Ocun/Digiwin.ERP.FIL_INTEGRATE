//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/10 17:19:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>获取品号信息服务</description>

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using System.Data;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IGetItemService))]
    [Description("根据传入的条码，获取品号信息")]
    public sealed class GetItemServicecs:ServiceComponent,IGetItemService {
        #region 相关服务

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer EncodeSrv {
            get {
                if (_encodeSrv == null)
                    _encodeSrv = this.GetService<IInfoEncodeContainer>();

                return _encodeSrv;
            }
        }

        #endregion

        #region IGetItemService 成员

        /// <summary>
        /// 根据传入的条码，获取品号信息
        /// </summary>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="itemId">品号集合(可能是单个品号,也可能是一个集合)</param>
        /// <param name="mainOrganization">主营组织</param>
        /// <param name="siteNo">工厂编号</param>
        /// <param name="mainOrganizationType">组织类型</param>
        /// <returns></returns>
        public DependencyObjectCollection GetItem(string programJobNo, DependencyObjectCollection itemId, object mainOrganization
            , string siteNo, string mainOrganizationType) {
            #region 参数检查
            if (string.IsNullOrEmpty(programJobNo)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "programJobNo" }));
            }
            if (itemId == null || itemId.Count <= 0) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "item_id" }));
            }
            if (string.IsNullOrEmpty(siteNo)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "site_no" }));
            }
            if (Maths.IsEmpty(mainOrganization)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "main_organization" }));
            }
            if (string.IsNullOrEmpty(mainOrganizationType)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "main_organization_type" }));
            }
            #endregion

            DependencyObjectCollection dt = null;
            if (mainOrganizationType == "PLANT")
                dt = GetItemForPlant(itemId, siteNo, programJobNo);
            else if (mainOrganizationType == "SUPPLY_CENTER")
                dt = GetItemForSupplyCenter(itemId, siteNo, mainOrganization, programJobNo);
            else if (mainOrganizationType == "SALES_CENTER")
                dt = GetItemForSalesCenter(itemId, siteNo, mainOrganization, programJobNo);
            else
                dt = CreateEmptyCollection();
            return dt;
        }

        #endregion

        #region 自定义方法

        /// <summary>
        /// 根据品号获取品号工厂信息
        /// </summary>
        /// <param name="itemID">品号ID集合</param>
        /// <param name="sitNo">工厂ID</param>
        /// <returns></returns>
        private DependencyObjectCollection GetItemForPlant(DependencyObjectCollection itemID, string siteNo, string programJobNo) {
            List<QueryProperty> properties = GetQueryProperty(siteNo);

            properties.Add(Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("ITEM_PLANT.RECEIPT_OVERRUN_RATE"), new CaseItem[]{
                new CaseItem(OOQL.CreateConstants(programJobNo).Like(OOQL.CreateConstants("7%"))
                    ,OOQL.CreateProperty("ITEM_PLANT.ISSUE_OVERRUN_RATE")),
                new CaseItem(OOQL.CreateConstants(programJobNo).Like(OOQL.CreateConstants("8%"))
                    ,OOQL.CreateProperty("ITEM_PLANT.ISSUE_SHORTAGE_RATE"))
            }), OOQL.CreateConstants(0m, GeneralDBType.Decimal), "over_deliver_rate"));  //超交率

            QueryNode node = OOQL.Select(
                    properties
                )
                .From("ITEM", "ITEM")
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID"))
                .LeftJoin("PLANT")
                .On(OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where(OOQL.AuthFilter("ITEM", "ITEM")
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)
                    & OOQL.CreateProperty("ITEM.ITEM_ID").In(OOQL.CreateDyncParameter("IDS", itemID.Select(c => c["item_id"]).ToArray())));

            return UtilsClass.ExecuteDependencyObject(this.GetService<IQueryService>(),node);
        }

        /// <summary>
        /// 根据品号获取品号采购信息
        /// </summary>
        /// <param name="itemID">品号ID集合</param>
        /// <param name="mainOrganization">采购域</param>
        /// <returns></returns>
        private DependencyObjectCollection GetItemForSupplyCenter(DependencyObjectCollection itemID, string siteNo, object mainOrganization, string programJobNo) {
            List<QueryProperty> properties = GetQueryProperty(siteNo);

            properties.Add(Formulas.IsNull(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"), OOQL.CreateConstants(0m, GeneralDBType.Decimal)
                , "over_deliver_rate"));   //超交率
            QueryNode node = OOQL.Select(
                    properties
                )
                .From("ITEM", "ITEM")
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PURCHASE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID"))
                .Where(OOQL.AuthFilter("ITEM", "ITEM")
                    & OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid") == OOQL.CreateConstants(mainOrganization)
                    & OOQL.CreateProperty("ITEM.ITEM_ID").In(OOQL.CreateDyncParameter("IDS", itemID.Select(c => c["item_id"]).ToArray())));

            return UtilsClass.ExecuteDependencyObject(this.GetService<IQueryService>(), node);
        }

        /// <summary>
        /// 根据品号获取品号销售信息
        /// </summary>
        /// <param name="itemID">品号ID集合</param>
        /// <param name="mainOrganization">销售域</param>
        /// <returns></returns>
        private DependencyObjectCollection GetItemForSalesCenter(DependencyObjectCollection itemID, string siteNo, object mainOrganization, string programJobNo) {
            List<QueryProperty> properties = GetQueryProperty(siteNo);

            properties.Add(Formulas.IsNull(OOQL.CreateProperty("ITEM_SALES.GENERAL_DEL_OVERRUN_RATE"), OOQL.CreateConstants(0m, GeneralDBType.Decimal)
                , "over_deliver_rate"));   //超交率
            
            QueryNode node = OOQL.Select(
                    properties
                )
                .From("ITEM", "ITEM")
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("ITEM_SALES")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_SALES.ITEM_ID"))
                .Where(OOQL.AuthFilter("ITEM", "ITEM")
                    & OOQL.CreateProperty("ITEM_SALES.Owner_Org.ROid") == OOQL.CreateConstants(mainOrganization)
                    & OOQL.CreateProperty("ITEM.ITEM_ID").In(OOQL.CreateDyncParameter("IDS", itemID.Select(c => c["item_id"]).ToArray())));

            return UtilsClass.ExecuteDependencyObject(this.GetService<IQueryService>(), node);
        }

        private List<QueryProperty> GetQueryProperty(string siteNo) {
            List<QueryProperty> properties = OOQL.CreateProperties("ITEM.ITEM_CODE|item_no"  //品号
                    , "ITEM.ITEM_NAME|item_name"  //品名
                    , "ITEM.ITEM_SPECIFICATION|item_spec"  //规格
                    , "ITEM_FEATURE.ITEM_FEATURE_CODE|item_feature_no"  //特征码
                    , "ITEM_FEATURE.ITEM_SPECIFICATION|item_feature_name"  //特征码规格
                    , "UNIT.UNIT_CODE|inventory_unit"  //单位编号
            ).ToList<QueryProperty>();

            //常量
            properties.Add(OOQL.CreateConstants("99", "enterprise_no"));  //企业编号
            properties.Add(OOQL.CreateConstants(siteNo, "site_no"));  //营运据点
            properties.Add(OOQL.CreateConstants("", "warehouse_no"));  //库位编号
            properties.Add(OOQL.CreateConstants("", "storage_spaces_no"));  //储位编号
            properties.Add(OOQL.CreateConstants("", "last_transaction_date"));  //最后异动日期
            //properties.Add(OOQL.CreateConstants(0, "result"));  //状态码
            //properties.Add(OOQL.CreateConstants("", "message"));  //错误信息

            return properties;
        }

        /// <summary>
        /// 组建空记录
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateEmptyCollection() {
            string[] columnNames = new string[] { 
                "item_no",
                "item_name",
                "item_spec",
                "item_feature_no",
                "item_feature_name",
                "inventory_unit",
                "enterprise_no",
                "site_no",
                "warehouse_no",
                "storage_spaces_no",
                "last_transaction_date"
                //"result",
                //"message"
            };
            DependencyObjectType rtnType = new DependencyObjectType("resultItem");
            foreach (string item in columnNames)
                rtnType.RegisterSimpleProperty(item, typeof(string));

            DependencyObjectCollection rtnColl = new DependencyObjectCollection(rtnType);

            return rtnColl;
        }

        #endregion
    }
}
