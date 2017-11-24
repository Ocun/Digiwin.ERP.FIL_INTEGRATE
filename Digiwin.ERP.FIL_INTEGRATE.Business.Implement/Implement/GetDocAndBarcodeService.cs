//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-12-23</createDate>
//<description>获取单据及条码服务</description>
//---------------------------------------------------------------- 
//20161229 modi by shenbao for P001-161215001
//20170302 modi by shenbao for P001-170302002 误差率统一乘100
//20170328 modi by wangyq for P001-170327001
//20170424 modi by wangyq for P001-170420001
//20170504 modi by wangyq for P001-170427001

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取单据及条码服务
    /// </summary>
    [ServiceClass(typeof(IGetDocAndBarcodeService))]
    [Description("获取单据及条码服务")]
    public class GetDocAndBarcodeService : ServiceComponent, IGetDocAndBarcodeService {
        #region IGetDocAndBarcodeService接口成员

        /// <summary>
        /// 获取单据及条码服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="scan_type">扫描类型</param>
        /// <param name="analysis_symbol">解析符号</param>
        /// <param name="status">执行动作</param>
        /// <param name="barcode_no">条形码编号</param>
        /// <param name="warehouse_no">库位编号</param>
        /// <param name="storage_spaces_no">储位编号</param>
        /// <param name="lot_no">批号</param>
        /// <param name="inventory_management_features">库存管理特征</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        public Hashtable GetDocAndBarcode(string program_job_no, string scan_type, string analysis_symbol, string status,
             string barcode_no, string warehouse_no, string storage_spaces_no, string lot_no,
             string inventory_management_features, string site_no) {
            Hashtable hashTableResult = new Hashtable();  //返回值
            IQueryService queryService = GetService<IQueryService>();

            //A5147D53-ADCE-47BE-C57E-0FFA9B119080  、 70E2E888-EB12-4728-4518-0FFA9B26BB34
            object objQueryQtyBcPropertyId = GetBcPropertyId(queryService, "Sys_INV_Qty") ?? Maths.GuidDefaultValue();
            object objQueryItemLotBcPropertyId = GetBcPropertyId(queryService, "Sys_Item_Lot") ?? Maths.GuidDefaultValue();

            if (program_job_no == "1" || program_job_no == "3") {
                //1.采购收货 3.收货入库

                DependencyObjectCollection collQuerySource = GetPurchaseOrder(queryService, barcode_no, site_no, program_job_no, status);
                if (collQuerySource.Count > 0) {
                    hashTableResult.Add("source_doc_detail", collQuerySource);
                }

                DependencyObjectCollection collQueryBarcode = GetBcRecord(queryService, barcode_no, site_no,
                    program_job_no, objQueryQtyBcPropertyId, objQueryItemLotBcPropertyId);
                if (collQueryBarcode.Count > 0) {
                    //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============begin=============
                    if (collQueryBarcode.ItemDependencyObjectType.Properties.Contains("inventory_management_features")) {
                        foreach (DependencyObject barCode in collQueryBarcode) {
                            barCode["inventory_management_features"] = UtilsClass.SpaceValue;
                        }
                    }
                    //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============end=============
                    hashTableResult.Add("barcode_detail", collQueryBarcode);

                }
            } else if (program_job_no == "9") {
                //9.完工入库GetMoProduct

                DependencyObjectCollection collQuerySource = GetMoProduct(queryService, barcode_no, site_no, program_job_no, status);
                if (collQuerySource.Count > 0) {
                    hashTableResult.Add("source_doc_detail", collQuerySource);
                }

                DependencyObjectCollection collQueryBarcode = GetBcRecord2(queryService, barcode_no, site_no,
                    program_job_no, objQueryQtyBcPropertyId, objQueryItemLotBcPropertyId);
                if (collQueryBarcode.Count > 0) {
                    //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============begin=============
                    if (collQueryBarcode.ItemDependencyObjectType.Properties.Contains("inventory_management_features")) {
                        foreach (DependencyObject barCode in collQueryBarcode) {
                            barCode["inventory_management_features"] = UtilsClass.SpaceValue;
                        }
                    }
                    //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============end=============
                    hashTableResult.Add("barcode_detail", collQueryBarcode);
                }
            }

            return hashTableResult.Keys.Count > 0 ? hashTableResult : null;
        }

        #endregion

        #region 业务方法

        /// <summary>
        /// 查询采购订单
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="barcodeNo">条形码编号</param>
        /// <param name="siteNo">营运据点</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private DependencyObjectCollection GetPurchaseOrder(IQueryService queryService, string barcodeNo, string siteNo, string programJobNo, string status) {
            QueryNode node =
                OOQL.Select(true, OOQL.CreateConstants("99", "enterprise_no"), //企业编号//20170328 modi by wangyq for P001-170327001 添加distinct
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"), //营运据点
                            OOQL.CreateConstants(programJobNo, "source_operation"), //来源作业
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "source_no"), //来源单号
                            OOQL.CreateConstants(programJobNo + status, "doc_type"), //单据类型  //20161229 modi by shenbao for P001-161215001
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_DATE", "create_date"), // 单据日期
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "seq"), //单据项次
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber", "doc_line_seq"), //单据项序
                            OOQL.CreateConstants(0, "doc_batch_seq"), //单据分批序
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"), //料件编号
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(""), "item_feature_no"), //产品特征
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(""), "item_feature_name"), //产品特征说明
                            OOQL.CreateConstants("", "warehouse_no"), //库位
                            OOQL.CreateConstants("", "storage_spaces_no"), //储位
                            OOQL.CreateConstants("", "lot_no"), // 批号
                            OOQL.CreateConstants("", "object_no"), // 对象编号
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.BUSINESS_QTY", "doc_qty"), //单据数量
                            OOQL.CreateProperty("PURCHASE_ORDER_SD.ARRIVED_BUSINESS_QTY", "in_out_qty"), //出入数量
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "unit_no"), //单位
                            OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"), //品名
                            OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"), //规格
                            OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_PURCHASE.RECEIPT_OVER_RATE"), OOQL.CreateConstants(0m, GeneralDBType.Decimal))
                                , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"), //允许误差率  //20170302 modi by shenbao for P001-170302002 误差率统一乘100
                            Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"),OOQL.CreateConstants("")) == OOQL.CreateConstants("N"),
                                        OOQL.CreateConstants("2"))
                                },
                                "lot_control_type"),
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator",
                                new object[]{
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                    OOQL.CreateConstants(1)
                                }), //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular",
                                new object[]{
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"),
                                    OOQL.CreateConstants(0)
                                }), //单位转换率分子
                            Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "inventory_unit"), //库存单位
                            OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_CODE", "main_organization"), //主营组织  //20161229 modi by shenbao for P001-161215001 PLANT_CODE==>SUPPLY_CENTER_CODE
                            OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                            OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type")//20170424 add by wangyq for P001-170420001
                            )
                    .From("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID"))
                //20161229 add by shenbao for P001-161215001 ===begin===
                    .InnerJoin("SUPPLY_CENTER")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid") == OOQL.CreateProperty("SUPPLY_CENTER.SUPPLY_CENTER_ID"))
                //20161229 add by shenbao for P001-161215001 ===end===
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid"))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER_D.ITEM_FEATURE_ID"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.BUSINESS_UNIT_ID"))
                    .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")  //20161229 add by shenbao for P001-161215001
                        & OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid"))
                    .LeftJoin("ITEM_PURCHASE", "ITEM_PURCHASE")
                    .On(OOQL.CreateProperty("ITEM_PURCHASE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        OOQL.CreateProperty("ITEM_PURCHASE.Owner_Org.ROid") ==
                        OOQL.CreateProperty("PURCHASE_ORDER.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("STOCK_UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"))
                //20170504 modi by wangyq for P001-170427001  ===============begin====================
                    .InnerJoin("BC_RECORD", "BC_RECORD")
                    .On(OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") ==
                        OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"))
                    .Where(OOQL.AuthFilter("PURCHASE_ORDER", "PURCHASE_ORDER") &
                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo) &

                    //.LeftJoin("BC_RECORD", "BC_RECORD")
                //.On(OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") ==
                //    OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"))
                //.Where(Formulas.IsNull(OOQL.CreateProperty("BC_RECORD.BARCODE_NO"), OOQL.CreateConstants("")) == OOQL.CreateConstants(barcodeNo) &
                //20170504 modi by wangyq for P001-170427001  ===============end====================
                           OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo) &
                           OOQL.CreateProperty("PURCHASE_ORDER.ApproveStatus") == OOQL.CreateConstants("Y"));


            DependencyObjectCollection coll = queryService.ExecuteDependencyObject(node);
            return coll;
        }


        /// <summary>
        /// 查询条码信息档
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="barcodeNo">条形码编号</param>
        /// <param name="siteNo">营运据点</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="objQueryQtyBcPropertyId"></param>
        /// <param name="objQueryItemLotBcPropertyId"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetBcRecord(IQueryService queryService, string barcodeNo, string siteNo, string programJobNo, object objQueryQtyBcPropertyId, object objQueryItemLotBcPropertyId) {
            QueryNode node =
                OOQL.Select(OOQL.CreateConstants("99", "enterprise_no"), //企业编号
                            OOQL.CreateConstants(siteNo, "site_no"), //营运据点
                            OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"), // 条码编号
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"), //料件编号
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(""), "item_feature_no"), //产品特征
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(""), "item_feature_name"), //产品特征说明
                            OOQL.CreateConstants("", "warehouse_no"), //库位
                            OOQL.CreateConstants("", "storage_spaces_no"), //储位
                            Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_VALUE"), OOQL.CreateConstants(""), "lot_no"), // 批号
                            Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_VALUE"), OOQL.CreateConstants(""), "barcode_lot_no"), //条码批号 //20161229 add by shenbao for P001-161215001
                            OOQL.CreateConstants(UtilsClass.SpaceValue, "inventory_management_features"), //库存管理特征//20170328 modi by wangyq for P001-170327001 old:""
                            OOQL.CreateProperty("UNIT.UNIT_CODE", "inventory_unit"), //库存单位
                            Formulas.Cast(Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"),OOQL.CreateConstants("")) != OOQL.CreateConstants(""),
                                        OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"))
                                },
                                string.Empty), GeneralDBType.Decimal, 20, 8, "barcode_qty"),//条码数量
                            OOQL.CreateConstants(0, "inventory_qty"), //库存数量
                            OOQL.CreateConstants(programJobNo, "source_operation"), //来源作业
                            OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO", "source_no"), //来源单号
                            OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber", "source_seq"), //来源项次
                            Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber"), OOQL.CreateConstants(0, GeneralDBType.Int32), "source_line_seq"), //来源项序
                            OOQL.CreateConstants(0, "source_batch_seq"), //来源分批序
                            OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"), //最后异动时间
                            OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"), //条码类型
                            OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"), //品名
                            OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"), //规格
                            Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"),OOQL.CreateConstants("")) == OOQL.CreateConstants("N"),
                                        OOQL.CreateConstants("2"))
                                },
                                "lot_control_type")
                            ) //批号管理
                    .From("BC_RECORD", "BC_RECORD")
                    .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                    .On(OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") &
                        OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.CreateConstants(objQueryQtyBcPropertyId, GeneralDBType.Guid))
                    .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D_02")
                    .On(OOQL.CreateProperty("BC_RECORD_D_02.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") &
                        OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_ID") == OOQL.CreateConstants(objQueryItemLotBcPropertyId, GeneralDBType.Guid))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID"))
                    .InnerJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"))
                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD", "PURCHASE_ORDER_SD")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid"))
                    .InnerJoin("PURCHASE_ORDER.PURCHASE_ORDER_D", "PURCHASE_ORDER_D")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID"))
                    .InnerJoin("PURCHASE_ORDER", "PURCHASE_ORDER")
                    .On(OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") ==
                        OOQL.CreateProperty("PURCHASE_ORDER_SD.RECEIVE_Owner_Org.ROid"))
                    .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .Where(OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo) &
                           OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));

            DependencyObjectCollection coll = queryService.ExecuteDependencyObject(node);
            return coll;
        }


        /// <summary>
        /// 查询条码信息档
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="barcodeNo">条形码编号</param>
        /// <param name="siteNo">营运据点</param>
        /// <param name="programJobNo">作业编号</param>
        /// <param name="objQueryQtyBcPropertyId"></param>
        /// <param name="objQueryItemLotBcPropertyId"></param>
        /// <returns></returns>
        private DependencyObjectCollection GetBcRecord2(IQueryService queryService, string barcodeNo, string siteNo, string programJobNo, object objQueryQtyBcPropertyId, object objQueryItemLotBcPropertyId) {
            QueryNode node =
                OOQL.Select(OOQL.CreateConstants("99", "enterprise_no"), //企业编号
                            OOQL.CreateConstants(siteNo, "site_no"), //营运据点
                            OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"), // 条码编号
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"), //料件编号
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(""), "item_feature_no"), //产品特征
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(""), "item_feature_name"), //产品特征说明
                            OOQL.CreateConstants("", "warehouse_no"), //库位
                            OOQL.CreateConstants("", "storage_spaces_no"), //储位
                            Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_VALUE"), OOQL.CreateConstants(""), "lot_no"), // 批号
                            Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_VALUE"), OOQL.CreateConstants(""), "barcode_lot_no"), //条码批号 //20161229 add by shenbao for P001-161215001 
                            OOQL.CreateConstants(UtilsClass.SpaceValue, "inventory_management_features"), //库存管理特征//20170328 modi by wangyq for P001-170327001 old:""
                            OOQL.CreateProperty("UNIT.UNIT_CODE", "inventory_unit"), //库存单位
                            Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"),OOQL.CreateConstants("")) != OOQL.CreateConstants(""),
                                        OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"))
                                },
                                "barcode_qty"),//条码数量
                            OOQL.CreateConstants(0, "inventory_qty"), //库存数量
                            OOQL.CreateConstants(programJobNo, "source_operation"), //来源作业
                            OOQL.CreateProperty("MO.DOC_NO", "source_no"), //来源单号
                            OOQL.CreateProperty("MO_PRODUCT.SequenceNumber", "source_seq"), //来源项次
                            OOQL.CreateConstants(0, "source_line_seq"), //来源项序
                            OOQL.CreateConstants(0, "source_batch_seq"), //来源分批序
                            OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"), //最后异动时间
                            OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"), //条码类型
                            OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"), //品名
                            OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"), //规格
                            Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"),OOQL.CreateConstants("")) == OOQL.CreateConstants("N"),
                                        OOQL.CreateConstants("2"))
                                },
                                "lot_control_type")
                            ) //批号管理
                    .From("BC_RECORD", "BC_RECORD")
                    .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                    .On(OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") &
                        OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_ID") == OOQL.CreateConstants(objQueryQtyBcPropertyId, GeneralDBType.Guid))
                    .LeftJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D_02")
                    .On(OOQL.CreateProperty("BC_RECORD_D_02.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") &
                        OOQL.CreateProperty("BC_RECORD_D_02.BC_PROPERTY_ID") == OOQL.CreateConstants(objQueryItemLotBcPropertyId, GeneralDBType.Guid))
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID"))
                    .InnerJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"))
                    .LeftJoin("MO.MO_PRODUCT", "MO_PRODUCT")
                    .On(OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid"))
                    .InnerJoin("MO", "MO")
                    .On(OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_PRODUCT.MO_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                    .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .Where(OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo) &
                           OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo));

            DependencyObjectCollection coll = queryService.ExecuteDependencyObject(node);
            return coll;
        }


        /// <summary>
        /// 查询工单产出信息
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="barcodeNo">条形码编号</param>
        /// <param name="siteNo">营运据点</param>
        /// <param name="programJobNo">作业编号</param>
        /// <returns></returns>
        private DependencyObjectCollection GetMoProduct(IQueryService queryService, string barcodeNo, string siteNo, string programJobNo, string status) {
            QueryNode node =
                OOQL.Select(true, OOQL.CreateConstants("99", "enterprise_no"), //企业编号//20170328 modi by wangyq for P001-170327001 添加distinct
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "site_no"), //营运据点
                            OOQL.CreateConstants(programJobNo, "source_operation"), //来源作业
                            OOQL.CreateProperty("MO.DOC_NO", "source_no"), //来源单号
                            OOQL.CreateConstants(programJobNo + status, "doc_type"), //单据类型
                            OOQL.CreateProperty("MO.DOC_DATE", "create_date"), // 单据日期
                            OOQL.CreateProperty("MO_PRODUCT.SequenceNumber", "seq"), //单据项次
                            OOQL.CreateConstants(0, GeneralDBType.Int32, "doc_line_seq"), //单据项序
                            OOQL.CreateConstants(0, "doc_batch_seq"), //单据分批序
                            OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"), //料件编号
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"), OOQL.CreateConstants(""), "item_feature_no"), //产品特征
                            Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"), OOQL.CreateConstants(""), "item_feature_name"), //产品特征说明
                            OOQL.CreateConstants("", "warehouse_no"), //库位
                            OOQL.CreateConstants("", "storage_spaces_no"), //储位
                            OOQL.CreateConstants("", "lot_no"), // 批号
                            OOQL.CreateConstants("", "object_no"), // 对象编号
                            OOQL.CreateProperty("MO_PRODUCT.PLAN_QTY", "doc_qty"), //单据数量
                            OOQL.CreateProperty("MO_PRODUCT.COMPLETED_QTY", "in_out_qty"), //出入数量
                            Formulas.IsNull(OOQL.CreateProperty("UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "unit_no"), //单位
                            OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"), //品名
                            OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"), //规格
                            OOQL.CreateArithmetic(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.RECEIPT_OVERRUN_RATE"), OOQL.CreateConstants(0m, GeneralDBType.Decimal))
                                , OOQL.CreateConstants(100), ArithmeticOperators.Mulit, "allow_error_rate"), //允许误差率 //20170302 modi by shenbao for P001-170302002 误差率统一乘100
                            Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL"),OOQL.CreateConstants("")) == OOQL.CreateConstants("N"),
                                        OOQL.CreateConstants("2"))
                                },
                                "lot_control_type"),
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_denominator",
                                new object[]{
                                    OOQL.CreateProperty("MO_PRODUCT.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("MO_PRODUCT.UNIT_ID"),
                                    OOQL.CreateConstants(1)
                                }), //单位转换率分母
                            Formulas.Ext("UNIT_CONVERT_02", "conversion_rate_molecular",
                                new object[]{
                                    OOQL.CreateProperty("MO_PRODUCT.ITEM_ID"),
                                    OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"),
                                    OOQL.CreateProperty("MO_PRODUCT.UNIT_ID"),
                                    OOQL.CreateConstants(0)
                                }), //单位转换率分子
                            Formulas.IsNull(OOQL.CreateProperty("STOCK_UNIT.UNIT_CODE"), OOQL.CreateConstants(""), "inventory_unit"), //库存单位
                            OOQL.CreateProperty("PLANT.PLANT_CODE", "main_organization"), //主营组织
                            OOQL.CreateProperty("UNIT.DICIMAL_DIGIT", "decimal_places"),//20170424 add by wangyq for P001-170420001
                            OOQL.CreateConstants("1", GeneralDBType.String, "decimal_places_type")//20170424 add by wangyq for P001-170420001
                            )
                    .From("MO.MO_PRODUCT", "MO_PRODUCT")
                    .InnerJoin("MO", "MO")
                    .On(OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_PRODUCT.MO_ID"))
                //20170410 mark by wangrm SD口述===start===========
                //.LeftJoin("MO.MO_D", "MO_D")
                //.On(OOQL.CreateProperty("MO_D.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                //20170410 mark by wangrm SD口述===end===========
                    .InnerJoin("ITEM", "ITEM")
                    .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("MO_PRODUCT.ITEM_ID"))
                    .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                    .On(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("MO_PRODUCT.ITEM_FEATURE_ID"))
                    .InnerJoin("PLANT", "PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "UNIT")
                    .On(OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("MO_PRODUCT.UNIT_ID"))//20170410 modi by wangrm SD口述OLD:MO_D->MO_PRODUCT
                    .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                    .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("MO.Owner_Org.ROid"))
                    .LeftJoin("UNIT", "STOCK_UNIT")
                    .On(OOQL.CreateProperty("STOCK_UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID"))
                //20170504 modi by wangyq for P001-170427001  ===============begin====================
                    .InnerJoin("BC_RECORD", "BC_RECORD")
                    .On(OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") == OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID"))
                    .Where(OOQL.AuthFilter("MO.MO_PRODUCT", "MO_PRODUCT") &
                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo) &
                //.LeftJoin("BC_RECORD", "BC_RECORD")
                //.On(OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") == OOQL.CreateProperty("MO_PRODUCT.MO_PRODUCT_ID"))
                //.Where(Formulas.IsNull(OOQL.CreateProperty("BC_RECORD.BARCODE_NO"), OOQL.CreateConstants("")) == OOQL.CreateConstants(barcodeNo) &
                //20170504 modi by wangyq for P001-170427001  ===============end====================
                           OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo) &
                           OOQL.CreateProperty("MO.ApproveStatus") == OOQL.CreateConstants("Y"));


            DependencyObjectCollection coll = queryService.ExecuteDependencyObject(node);
            return coll;
        }

        /// <summary>
        /// 查询条码规则属性项
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="bcPropertyCode">属性项编码</param>
        /// <returns></returns>
        private object GetBcPropertyId(IQueryService queryService, string bcPropertyCode) {
            QueryNode node =
                OOQL.Select(OOQL.CreateProperty("BC_PROPERTY_ID"))
                    .From("BC_PROPERTY")
                    .Where(OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants(bcPropertyCode));

            object result = queryService.ExecuteScalar(node);  //20161229 add by shenbao for P001-161215001

            return result;
        }

        #endregion
    }
}