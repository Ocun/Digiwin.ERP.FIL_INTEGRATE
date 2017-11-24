//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/12/23 13:49:37</createDate>
//<IssueNo>P001-161215001</IssueNo>
//<description>取得出货指示服务</description>
//20170328 modi by wangyq for P001-170327001
//20170629 modi by zhangcn for P001-170606002 增加采购退货出库单
//20170717 modi by shenbao for P001-170717001
//20170829 modi by shenbao for P001-170717001 增加寄售订单
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IInstructionsService))]
    [Description("取得出货指示服务")]
    public sealed class InstructionsService : ServiceComponent, IInstructionsService {
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

        #region IInstructionsService 成员

        /// <summary>
        /// 取得出货指示
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">A.新增  S.过帐</param>
        /// <param name="doc_no">单据编号</param>
        /// <param name="site_no">工厂</param>
        /// <param name="warehouse_no">库位编号</param>
        /// <returns></returns>
        public Hashtable GetInstructions(string program_job_no, string status, DependencyObjectCollection param_master, string site_no, string warehouse_no) {
            #region 参数检查
            if (Maths.IsEmpty(program_job_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "program_job_no" }));
            }
            if (Maths.IsEmpty(status)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "status" }));
            }
            if (Maths.IsEmpty(param_master) || param_master.Count <= 0) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "doc_no" }));
            }
            if (Maths.IsEmpty(site_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "site_no" }));
            }
            #endregion

            QueryNode node = QueryData(program_job_no, status, param_master, site_no, warehouse_no, "1");
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "2"), true);
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "3"), true);
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "4"), true);
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "5"), true);
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "6"), true);
            node = new UnionNode(node, QueryData(program_job_no, status, param_master, site_no, warehouse_no, "7"), true);

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============begin=============
            if (result.Count > 0 && result.ItemDependencyObjectType.Properties.Contains("inventory_management_features")) {
                foreach (DependencyObject barCode in result) {
                    barCode["inventory_management_features"] = UtilsClass.SpaceValue;
                }
            }
            //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============end=============
            Hashtable hashTable = new Hashtable();
            hashTable.Add("inventory_detail", result);

            return hashTable;
        }

        #endregion

        #region 自定义方法

        private QueryNode QueryData(string program_job_no, string status,
            DependencyObjectCollection doc_no, string site_no, string warehouse_no, string warseHouseControl) {
            OrderByItem[] orderByItem = null;
            QueryProperty property = null;
            bool bcInvControl = UtilsClass.IsBCInventoryManagement(this.GetService<IQueryService>());
            if (bcInvControl)
                property = OOQL.CreateProperty("BC_INVENTORY.QTY");
            else
                property = OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY");

            //策略
            if (warseHouseControl == "1")  //1.生效日期先进先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem("ITEM_LOT.EFFECTIVE_DATE", SortType.Asc) };
            else if (warseHouseControl == "2")  //2.生效日期后进先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem("ITEM_LOT.EFFECTIVE_DATE", SortType.Desc) };
            else if (warseHouseControl == "3")  //3.不限定
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem("ITEM_LOT.LOT_CODE", SortType.Asc) };
            else if (warseHouseControl == "4")  //4.先到期先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem("ITEM_LOT.INEFFECTIVE_DATE", SortType.Asc) };
            else if (warseHouseControl == "5")  //5.允许出库日早者先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem("ITEM_LOT.ALLOW_ISSUE_DATE", SortType.Asc) };
            else if (warseHouseControl == "6") {  //6.可用量少的先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem(property, SortType.Asc) };
            } else if (warseHouseControl == "7") {  //6.可用量少的先出
                orderByItem = new OrderByItem[] { OOQL.CreateOrderByItem(property, SortType.Desc) };
            }

            //组织单号
            List<ConstantsQueryProperty> docNos = new List<ConstantsQueryProperty>();
            foreach (DependencyObject item in doc_no)
                docNos.Add(OOQL.CreateConstants(item["doc_no"].ToStringExtension()));

            QueryNode node = null;
            if (program_job_no == "5")//20170328 modi by wangyq for P001-170327001去掉&& status == "A"
                node = GetSalesDeliveryNode(site_no, docNos, warseHouseControl);
            else if (program_job_no.StartsWith("7") && status == "A")
                node = GetMONode(site_no, program_job_no, docNos, warseHouseControl);
            else if (program_job_no.StartsWith("7") && status == "S")
                node = GetIssueReceiptNode(site_no, docNos, warseHouseControl);
            else if (program_job_no.StartsWith("11") && status == "S")
                node = GeTransferDocNode(site_no, docNos, warseHouseControl);
            else if (program_job_no.StartsWith("4"))//20170629 add by zhangcn for P001-170606002  //20170719 modi by shenbao for 拿掉status == "A"
                node = GePurchaseReturnNode(site_no, docNos, warseHouseControl);//20170629 add by zhangcn for P001-170606002
            else if (program_job_no.StartsWith("5-1"))  //20170829 add by shenbao for P001-170717001
                node = GetSalesOrderDocNode(site_no, docNos, warseHouseControl);

            List<QueryProperty> pubProperties = new List<QueryProperty>() {
                OOQL.CreateConstants("99","enterprise_no"),
                OOQL.CreateProperty("QuerySource.PLANT_CODE", "site_no"),
                OOQL.CreateProperty("QuerySource.ITEM_CODE", "item_no"),
                OOQL.CreateProperty("QuerySource.ITEM_NAME", "item_name"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.ITEM_SPECIFICATION"),OOQL.CreateConstants(string.Empty), "item_spec"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.ITEM_FEATURE_CODE"),OOQL.CreateConstants(string.Empty), "item_feature_no"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.ITEM_FEATURE_SPECIFICATION"),OOQL.CreateConstants(string.Empty), "item_feature_name"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.UNIT_CODE"),OOQL.CreateConstants(string.Empty),"unit_no"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.STOCK_UNIT_CODE"),OOQL.CreateConstants(string.Empty),"inventory_unit"),
                Formulas.IsNull(OOQL.CreateProperty("QuerySource.SOURCE_QTY"),OOQL.CreateConstants(0),"SOURCE_QTY")
            };

            //查询
            if (bcInvControl) {
                pubProperties.AddRange(new QueryProperty[]{
                    OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO", "barcode_no"),
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"), OOQL.CreateConstants(string.Empty), "warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty), "storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"), OOQL.CreateConstants(string.Empty), "lot_no"),
                    OOQL.CreateConstants(UtilsClass.SpaceValue, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:string.Empty
                    Formulas.IsNull(OOQL.CreateProperty("BC_INVENTORY.CreateDate"), OOQL.CreateConstants(OrmDataOption.EmptyDateTime.Date),"first_storage_date"),
                    Formulas.IsNull(OOQL.CreateProperty("BC_INVENTORY.QTY"), OOQL.CreateConstants(0),"inventory_qty"),
                    Formulas.IsNull(Formulas.Ext("UNIT_CONVERT", new object[]{ OOQL.CreateProperty("QuerySource.ITEM_ID")
                        , OOQL.CreateProperty("QuerySource.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("BC_INVENTORY.QTY")
                        , OOQL.CreateProperty("QuerySource.UNIT_ID")
                        , OOQL.CreateConstants(0)}),OOQL.CreateConstants(0),"conversion_qty"),
                    Formulas.RowNumber("sort_no", OOQL.Over(new QueryProperty[] { OOQL.CreateProperty("QuerySource.ITEM_CODE") },
                         orderByItem
                     ))
                });
                node = OOQL.Select(
                        pubProperties
                    )
                    .From(node, "QuerySource")
                    .LeftJoin("BC_INVENTORY")
                    .On(OOQL.CreateProperty("QuerySource.ITEM_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_ID")
                        & Formulas.IsNull(OOQL.CreateProperty("QuerySource.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("BC_INVENTORY.ITEM_FEATURE_ID"))
                    .LeftJoin("WAREHOUSE")
                    .On(OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("BC_INVENTORY.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                    .LeftJoin("ITEM_LOT")
                    .On(OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                    .LeftJoin("PLANT")  //20170719 add by shenbao for P001-170717001
                    .On(OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .Where(((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(warehouse_no))
                        | OOQL.CreateConstants(warehouse_no) == OOQL.CreateConstants(""))
                        & OOQL.CreateProperty("BC_INVENTORY.QTY") > OOQL.CreateConstants(0)
                        & OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_PROPERTY") == OOQL.CreateConstants("1")  //20170717 add by shenbao for P001-170717001
                        & OOQL.CreateProperty("WAREHOUSE.INCLUDED_AVAILABLE_QTY") == OOQL.CreateConstants(true) //20170717 add by shenbao for P001-170717001
                        & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)); //20170719 add by shenbao for P001-170717001
            } else {
                pubProperties.AddRange(new QueryProperty[]{
                    OOQL.CreateConstants("", "barcode_no"),
                    Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE"),OOQL.CreateConstants(string.Empty),"warehouse_no"),
                    Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_CODE"), OOQL.CreateConstants(string.Empty),"storage_spaces_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),OOQL.CreateConstants(string.Empty),"lot_no"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.LAST_RECEIPT_DATE"),OOQL.CreateConstants(OrmDataOption.EmptyDateTime.Date),"first_storage_date"),
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY"), OOQL.CreateConstants(0),"inventory_qty"),
                    Formulas.IsNull(Formulas.Ext("UNIT_CONVERT", new object[]{ OOQL.CreateProperty("QuerySource.ITEM_ID")
                        , OOQL.CreateProperty("QuerySource.STOCK_UNIT_ID")
                        , OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY")
                        , OOQL.CreateProperty("QuerySource.UNIT_ID")
                        , OOQL.CreateConstants(0)}),OOQL.CreateConstants(0),"conversion_qty"),
                    Formulas.RowNumber("sort_no", OOQL.Over(new QueryProperty[] { OOQL.CreateProperty("QuerySource.ITEM_CODE") },
                         orderByItem
                     ))
                });
                node = OOQL.Select(
                        pubProperties
                    )
                    .From(node, "QuerySource")
                    .LeftJoin("ITEM_WAREHOUSE_BIN")
                    .On(OOQL.CreateProperty("QuerySource.ITEM_ID") == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_ID")
                       & Formulas.IsNull(OOQL.CreateProperty("QuerySource.ITEM_FEATURE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())) == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_FEATURE_ID"))
                    .LeftJoin("WAREHOUSE")
                    .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_ID") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_ID") == OOQL.CreateProperty("BIN.BIN_ID"))
                    .LeftJoin("ITEM_LOT")
                    .On(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"))
                    .LeftJoin("PLANT")  //20170719 add by shenbao for P001-170717001
                    .On(OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .Where(((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(warehouse_no))
                        | OOQL.CreateConstants(warehouse_no) == OOQL.CreateConstants(""))
                        & OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY") > OOQL.CreateConstants(0)
                        & OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BO_ID.RTK") == OOQL.CreateConstants("OTHER")
                        & OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_PROPERTY") == OOQL.CreateConstants("1")  //20170717 add by shenbao for P001-170717001
                        & OOQL.CreateProperty("WAREHOUSE.INCLUDED_AVAILABLE_QTY") == OOQL.CreateConstants(true) //20170717 add by shenbao for P001-170717001
                        & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)); //20170719 add by shenbao for P001-170717001
            }

            return node;
        }

        /// <summary>
        /// 查询销货出库单信息
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <returns></returns>
        private QueryNode GetSalesDeliveryNode(string site_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("SALES_DELIVERY_D.BUSINESS_QTY"))
                        - Formulas.Sum(OOQL.CreateProperty("SALES_DELIVERY_D.ISSUED_QTY")), OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("SALES_DELIVERY", "SALES_DELIVERY")
                .InnerJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                .On(OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("SALES_DELIVERY.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("SALES_DELIVERY_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("SALES_DELIVERY.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("SALES_DELIVERY", "SALES_DELIVERY"))
                    & (OOQL.CreateProperty("SALES_DELIVERY.DOC_NO").In(docNos.ToArray())
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("SALES_DELIVERY.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("SALES_DELIVERY.CATEGORY") == OOQL.CreateConstants("24")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("SALES_DELIVERY_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));

            return node;
        }

        /// <summary>
        /// 查询工单信息
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <returns></returns>
        private QueryNode GetMONode(string site_no, string program_job_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("MO_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("MO_D.REQUIRED_QTY"))
                        - Formulas.Sum(OOQL.CreateProperty("MO_D.ISSUED_QTY")), OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("MO", "MO")
                .InnerJoin("MO.MO_D", "MO_D")
                .On(OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID"));

            //20160104 add by shenbao for P001-161215001 ===begin===
            if (program_job_no == "7-3") {
                node = ((JoinOnNode)node)
                .InnerJoin("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D", "ISSUE_RECEIPT_REQ_D")
                .On(OOQL.CreateProperty("MO_D.MO_D_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.MO_D_ID"))
                .InnerJoin("ISSUE_RECEIPT_REQ")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID"));
            }

            QueryCondition condition = null;
            if (program_job_no == "7-3") {
                condition = OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO").In(docNos.ToArray());
            } else {
                condition = OOQL.CreateProperty("MO.DOC_NO").In(docNos.ToArray());
            }
            //20160104 add by shenbao for P001-161215001 ===end===

            node = ((JoinOnNode)node)
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("MO_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("MO_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("MO_D.UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("MO_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("MO", "MO"))
                    & (condition  //20160104 modi by shenbao for P001-161215001 改为变量
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("MO_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));

            return node;
        }

        /// <summary>
        /// 查询领料出库单信息
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <returns></returns>
        private QueryNode GetIssueReceiptNode(string site_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_QTY")), OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                .InnerJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("ISSUE_RECEIPT.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("ISSUE_RECEIPT", "ISSUE_RECEIPT"))
                    & (OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO").In(docNos.ToArray())
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("ISSUE_RECEIPT.ApproveStatus") == OOQL.CreateConstants("N")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("ISSUE_RECEIPT_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));

            return node;
        }

        /// <summary>
        /// 查询库存交易单信息
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <returns></returns>
        private QueryNode GeTransferDocNode(string site_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_QTY")), OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("TRANSACTION_DOC", "TRANSACTION_DOC")
                .InnerJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                .On(OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("TRANSACTION_DOC_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("TRANSACTION_DOC.Owner_Org.ROid") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("TRANSACTION_DOC", "TRANSACTION_DOC"))
                    & (OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO").In(docNos.ToArray())
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("TRANSACTION_DOC.ApproveStatus") == OOQL.CreateConstants("N")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("TRANSACTION_DOC_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));

            return node;
        }

        //20170629 add by zhangcn for P001-170606002 ===begin===
        /// <summary>
        /// 查询采购退货单
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <returns></returns>
        private QueryNode GePurchaseReturnNode(string site_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node =  OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_QTY"))-
                                    Formulas.Sum(OOQL.CreateProperty("PURCHASE_RETURN_D.ISSUED_BUSINESS_QTY")), 
                                    OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("PURCHASE_RETURN", "PURCHASE_RETURN")
                .InnerJoin("PURCHASE_RETURN.PURCHASE_RETURN_D", "PURCHASE_RETURN_D")
                .On(OOQL.CreateProperty("PURCHASE_RETURN.PURCHASE_RETURN_ID") == OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT", "UNIT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("PURCHASE_RETURN.PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("PURCHASE_RETURN", "PURCHASE_RETURN"))
                    & (OOQL.CreateProperty("PURCHASE_RETURN.DOC_NO").In(docNos.ToArray())
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("PURCHASE_RETURN.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("PURCHASE_RETURN.CATEGORY") == OOQL.CreateConstants("39")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("PURCHASE_RETURN_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));
            return node;
        }
        //20170629 add by zhangcn for P001-170606002 ===end===

        //20170829 add by shenbao for P001-170717001 ===begin===
        /// <summary>
        /// 查询寄售调拨单
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="docNos"></param>
        /// <param name="warseHouseControl"></param>
        /// <returns></returns>
        private QueryNode GetSalesOrderDocNode(string site_no, List<ConstantsQueryProperty> docNos, string warseHouseControl) {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION", "ITEM_FEATURE_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE", "STOCK_UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID", "STOCK_UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                    Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("SALES_ORDER_DOC_SD.BUSINESS_QTY") - OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERED_BUSINESS_QTY"))
                    , OOQL.CreateConstants(0), "SOURCE_QTY")
                )
                .From("SALES_ORDER_DOC", "SALES_ORDER_DOC")
                .InnerJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D", "SALES_ORDER_DOC_D")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC.SALES_ORDER_DOC_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_ID"))
                .InnerJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_D_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_D_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"))
                .LeftJoin("UNIT")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.BUSINESS_UNIT_ID") == OOQL.CreateProperty("UNIT.UNIT_ID"))
                .LeftJoin("UNIT", "STOCK_UNIT")
                .On(OOQL.CreateProperty("ITEM.STOCK_UNIT_ID") == OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"))
                .LeftJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_PLANT_ID") == OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid"))
                .Where((OOQL.AuthFilter("SALES_ORDER_DOC", "SALES_ORDER_DOC"))
                    & (OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO").In(docNos.ToArray())
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no)
                    & OOQL.CreateProperty("SALES_ORDER_DOC.ApproveStatus") == OOQL.CreateConstants("Y")
                    & OOQL.CreateProperty("SALES_ORDER_DOC.CATEGORY") == OOQL.CreateConstants("2B")
                    & OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL") == OOQL.CreateConstants(warseHouseControl)
                    ))
                .GroupBy(OOQL.CreateProperty("PLANT.PLANT_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_CODE"),
                    OOQL.CreateProperty("ITEM.ITEM_NAME"),
                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"),
                    OOQL.CreateProperty("ITEM_PLANT.WAREHOUSE_CONTROL"),
                    OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_ID"),
                    OOQL.CreateProperty("UNIT.UNIT_ID"),
                    OOQL.CreateProperty("STOCK_UNIT.UNIT_ID"),
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"));

            return node;
        }
        //20170829 add by shenbao for P001-170717001 ===end===

        #endregion
    }
}
