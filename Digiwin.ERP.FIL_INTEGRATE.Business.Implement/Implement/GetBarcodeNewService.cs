//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2016-12-19</createDate>
//<description>获取条码服务实现</description>
//----------------------------------------------------------------
//20161229 modi by shenbao for P001-161215001
//20170111 modi by shenbao for P001-170111001
//20170215 modi by liwei1 for P001-170203001
//20170328 modi by wangyq for P001-170327001
//20170504 modi by wangyq for P001-170427001
//20170801 modi by shenbao for P001-170717001
//20170731 modi by liwei1 for P001-170717001 增加寄售调拨单
//20170906 modi by liwei1 for P001-170717001
//20170905 modi by wangyq for P001-170717001 

using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    /// <summary>
    /// 获取条码服务
    /// </summary>
    [ServiceClass(typeof(IGetBarcodeNewService))]
    [Description("获取条码服务")]
    public class GetBarcodeNewService : ServiceComponent, IGetBarcodeNewService {
        /// <summary>
        /// 获取条码接口
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <param name="barcode_no">条码编号</param>
        /// <param name="warehouse_no">仓库编号</param>
        /// <param name="storage_spaces_no">库位编号</param>
        /// <param name="lot_no">批号</param>
        /// <param name="inventory_management_features">库存管理特征</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        public Hashtable GetBarcodeNew(string program_job_no, string status, string barcode_no, string warehouse_no,
            string storage_spaces_no, string lot_no, string inventory_management_features, string site_no) {
            try {
                //根据工厂编号获取工厂ID
                object plantId = GetPlantId(site_no);

                //启用条码库存管理
                bool bcInventoryManagement = false;
                //存在Typekey：FIL参数(PARA_FIL)
                if (GetExistenceParaFil()) {
                    bcInventoryManagement = GetBcInventoryManagement();
                }

                QueryNode node = null;
                switch (program_job_no) {
                    case "2"://2.采购入库
                    case "2-1":  //20170111 add by shenbao for P001-170111001
                        node = QueryPurchaseReceipt(site_no, program_job_no, barcode_no, plantId);
                        break;
                    //20170215 add by liwei1 for P001-170203001 ===begin===
                    case "4"://4.采购退货
                        node = QueryPurchaseReturn(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    case "6"://6.销退单 
                        node = QuerySalesReturn(site_no, program_job_no, barcode_no, plantId);
                        break;
                    //20170215 add by liwei1 for P001-170203001 ===end===
                    case "5"://5.销售出货 
                        node = QuerySalesShipment(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;

                    //20170731 add by liwei1 for P001-170717001 ===begin===
                    case "5-1"://5-1.寄售调拨单
                        node = QuerySalesOrderDoc(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    //20170731 add by liwei1 for P001-170717001 ===end===
                    //20170905 add by wangyq for P001-170717001  =================begin===================
                    case "5-2":
                        node = QuerySalesShipment01(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    //20170905 add by wangyq for P001-170717001  =================end===================
                    case "7"://7.工单发料
                    case "7-5":  //20170905 add by wangyq for P001-170717001 
                        if (status == "A") {//A.新增
                            node = QueryMoStoreIssueA(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        } else if (status == "S") {//S.过账
                            node = QueryMoStoreIssueS(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        }
                        break;
                    case "7-3"://7-3.领料申请单
                        node = QueryIssueReceiptReq(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    case "8"://8.工单退料
                        if (status == "A") {//A.新增
                            node = QueryMoMaterialReturnA(site_no, program_job_no, barcode_no, plantId);
                        } else if (status == "S") {//S.过账
                            node = QueryMoMaterialReturnS(site_no, program_job_no, barcode_no, plantId);
                        }
                        break;
                    case "9-2"://9-2.生产入库单
                        node = QueryMoReceiptRequistion(site_no, program_job_no, barcode_no, plantId);
                        break;
                    case "9-3"://9-3.生产入库工单  //20170801 add by shenbao for P001-170717001
                        node = QueryMoReceiptMo(site_no, program_job_no, barcode_no, plantId);
                        break;
                    case "11"://11.杂项发料
                        node = QueryMiscellaneousIssues(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    case "12"://12.杂项收料
                        node = QueryMiscellaneousCharge(site_no, program_job_no, barcode_no, plantId);
                        break;
                    case "13-1":
                    case "13-2"://13-1.调拨单 OR 13-2.调出单
                        node = QueryTransferRequisition(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    case "13-3"://13-3.拨入单 
                        node = QueryDialInOrder(site_no, program_job_no, barcode_no, plantId);
                        break;
                    case "13-5"://13-5.调拨单
                        node = QueryTransferRequisitionDoc(site_no, program_job_no, barcode_no, warehouse_no, storage_spaces_no, lot_no, plantId, bcInventoryManagement);
                        break;
                    //20170328 add by wangyq for P001-170327001   =============begin=============
                    case "1-1"://1-1.依送货做采购收货
                    case "3-1"://3-1.依送货做收货入库
                        object objQueryQtyBcPropertyId = GetBcPropertyId("Sys_INV_Qty") ?? Maths.GuidDefaultValue();
                        object objQueryItemLotBcPropertyId = GetBcPropertyId("Sys_Item_Lot") ?? Maths.GuidDefaultValue();
                        node = GetBcRecord(barcode_no, site_no, program_job_no, objQueryQtyBcPropertyId, objQueryItemLotBcPropertyId);
                        break;
                    //20170328 add by wangyq for P001-170327001   =============end=============
                }

                //查询
                DependencyObjectCollection barcodeDetail = this.GetService<IQueryService>().ExecuteDependencyObject(node);
                //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============begin=============
                if (barcodeDetail.Count > 0 && barcodeDetail.ItemDependencyObjectType.Properties.Contains("inventory_management_features")) {
                    foreach (DependencyObject barCode in barcodeDetail) {
                        barCode["inventory_management_features"] = UtilsClass.SpaceValue;
                    }
                }
                //20170328 add by wangyq for P001-170327001   数据库中加入无效果,继续更新一次=============end=============
                //组织返回结果
                Hashtable result = new Hashtable();
                result.Add("barcode_detail", barcodeDetail);
                return result;
            } catch (System.Exception) {
                throw;
            }
        }

        #region 业务方法
        /// <summary>
        /// 是否存在【PARA_FIL】TypeKey
        /// </summary>
        /// <returns></returns>
        private bool GetExistenceParaFil() {
            ICreateService createSrv = this.GetService<ICreateService>("PARA_FIL");
            ITypeKeyMetadataContainer typeKeyMetadataCtr = this.GetService<ITypeKeyMetadataContainer>();
            TypeKeyMetadataCollection typekeyCollection = typeKeyMetadataCtr.TypeKeys;
            bool isOK = false;
            foreach (var item in typekeyCollection) {
                //判断结合中TypeKey是否存在PARA_FIL
                if (item.TypeKey == "PARA_FIL") {
                    isOK = true;
                    break;
                }
            }
            return isOK;
        }
        #endregion

        #region 数据库相关
        /// <summary>
        /// 库存数量列取值
        /// </summary>
        /// <param name="propertyList"></param>
        /// <param name="barcodeType"></param>
        /// <param name="bcInventoryManagement"></param>
        private void AddInventoryQty(List<QueryProperty> propertyList, string barcodeType, bool bcInventoryManagement) {
            string str = string.Empty;
            //启用条码库存管理
            if (bcInventoryManagement) {
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                propertyList.Add(Formulas.IsNull(
                         OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_CODE"),
                         OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
                propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.LOT_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "barcode_lot_no"));//20161229 add by shenbao for P001-161215001
                //if (barcodeType == "1" || barcodeType == "2") {//20170405 mark by wangrm SD口述
                if (barcodeType == "2") {//20170405 add by wangrm SD口述
                    propertyList.Add(
                        Formulas.Case(null,
                            Formulas.Case(null, OOQL.CreateConstants(0, GeneralDBType.Decimal), new CaseItem[]{
                                new CaseItem((OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))}),
                            OOQL.CreateCaseArray(
                                    OOQL.CreateCaseItem((Formulas.Case(null, OOQL.CreateConstants(0, GeneralDBType.Decimal), new CaseItem[]{
                                        new CaseItem((OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                            Formulas.Cast(OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))})
                                            > Formulas.IsNull(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.QTY"), OOQL.CreateConstants(0))),
                                            Formulas.IsNull(OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.QTY"), OOQL.CreateConstants(0)))), "inventory_qty"));
                } else {
                    propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.QTY"),
                        OOQL.CreateConstants(0, GeneralDBType.Int32), "inventory_qty"));
                }
            } else {
                #region //20170906 modi by liwei1 for P001-170717001 注释重写
                //if (barcodeType == "3") {
                //    propertyList.Add(Formulas.IsNull(
                //        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_CODE"),
                //        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                //    propertyList.Add(Formulas.IsNull(
                //             OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_CODE"),
                //             OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                //    propertyList.Add(Formulas.IsNull(
                //            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                //            OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
                //    propertyList.Add(Formulas.IsNull(
                //            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                //            OOQL.CreateConstants(str, GeneralDBType.String), "barcode_lot_no"));//20161229 add by shenbao for P001-161215001
                //    propertyList.Add(Formulas.IsNull(
                //        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY"),
                //        OOQL.CreateConstants(0, GeneralDBType.Decimal), "inventory_qty"));
                //} else {
                //    propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                //    propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                //    propertyList.Add(Formulas.IsNull(
                //            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                //            OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
                //    propertyList.Add(Formulas.IsNull(
                //            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                //            OOQL.CreateConstants(str, GeneralDBType.String), "barcode_lot_no"));//20161229 add by shenbao for P001-161215001
                //    propertyList.Add(Formulas.Case(null,
                //        OOQL.CreateConstants(1, GeneralDBType.Decimal),
                //        OOQL.CreateCaseArray(
                //                OOQL.CreateCaseItem(
                //                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                //                        Formulas.Cast(
                //                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"));
                //}
                #endregion

                #region//20170906 add by liwei1 for P001-170717001 重写上面逻辑
                if (barcodeType == "2") {
                    propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
                    propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
                    propertyList.Add(Formulas.IsNull(
                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                            OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
                    propertyList.Add(Formulas.IsNull(
                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                            OOQL.CreateConstants(str, GeneralDBType.String), "barcode_lot_no"));
                    propertyList.Add(Formulas.Case(null,
                        OOQL.CreateConstants(1, GeneralDBType.Decimal),
                        OOQL.CreateCaseArray(
                                OOQL.CreateCaseItem(
                                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                        Formulas.Cast(
                                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"));
                } else {
                    propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_CODE"),
                        OOQL.CreateConstants(str, GeneralDBType.String), "warehouse_no"));
                    propertyList.Add(Formulas.IsNull(
                             OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_CODE"),
                             OOQL.CreateConstants(str, GeneralDBType.String), "storage_spaces_no"));
                    propertyList.Add(Formulas.IsNull(
                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                            OOQL.CreateConstants(str, GeneralDBType.String), "lot_no"));
                    propertyList.Add(Formulas.IsNull(
                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"),
                            OOQL.CreateConstants(str, GeneralDBType.String), "barcode_lot_no"));
                    propertyList.Add(Formulas.IsNull(
                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY"),
                        OOQL.CreateConstants(0, GeneralDBType.Decimal), "inventory_qty"));
                }
                #endregion
            }
        }

        /// <summary>
        /// 根据是否启用条码库存管理增加关联
        /// </summary>
        /// <param name="joinOnNode"></param>
        /// <param name="barcodeType"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <returns></returns>
        private QueryNode AddJoinOnNode(JoinOnNode joinOnNode, string barcodeType, bool bcInventoryManagement, string warehouseNo, string storageSpacesNo, string lotNo, string barcodeNo) {
            QueryConditionGroup conditionGroup = (OOQL.CreateConstants(1) == OOQL.CreateConstants(1)) & (OOQL.CreateConstants(1) == OOQL.CreateConstants(1));
            if (!Maths.IsEmpty(warehouseNo)) {
                conditionGroup &= (OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(warehouseNo));
            }
            if (!Maths.IsEmpty(storageSpacesNo)) {
                conditionGroup &= (OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateConstants(storageSpacesNo));
            }
            if (!Maths.IsEmpty(lotNo)) {
                conditionGroup &= (OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(lotNo));
            }
            //启用条码库存管理
            if (bcInventoryManagement) {
                joinOnNode = joinOnNode.LeftJoin(
                                                        OOQL.Select(
                                                                    OOQL.CreateProperty("BC_INVENTORY.BARCODE_NO", "BARCODE_NO"),
                                                                    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE", "WAREHOUSE_CODE"),
                                                                    OOQL.CreateProperty("BIN.BIN_CODE", "BIN_CODE"),
                                                                    OOQL.CreateProperty("ITEM_LOT.LOT_CODE", "LOT_CODE"),
                                                                    OOQL.CreateProperty("BC_INVENTORY.QTY", "QTY"))
                                                                .From("BC_INVENTORY", "BC_INVENTORY")
                                                                .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                                                .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BC_INVENTORY.WAREHOUSE_ID")))
                                                                .LeftJoin("WAREHOUSE.BIN", "BIN")
                                                                .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("BC_INVENTORY.BIN_ID")))
                                                                .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                                                .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("BC_INVENTORY.ITEM_LOT_ID")))
                                                                .Where(conditionGroup), "ITEM_WAREHOUSE_BIN")
                                                        .On((OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BARCODE_NO"))
                                                           & (OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.QTY") > OOQL.CreateConstants(0, GeneralDBType.Int32)));
            } else {
                joinOnNode = joinOnNode.LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")));
                if (barcodeType != "2") {//20170906 add by liwei1 for P001-170717001  old：barcodeType == "3"
                    //关联查询存货余额明细信息
                    joinOnNode = joinOnNode.LeftJoin(
                                                            (OOQL.Select(
                                                                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_ID", "ITEM_ID"),
                                                                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_FEATURE_ID", "ITEM_FEATURE_ID"),
                                                                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BO_ID.RTK", "BO_ID"),
                                                                        OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE", "WAREHOUSE_CODE"),
                                                                        OOQL.CreateProperty("BIN.BIN_CODE", "BIN_CODE"),
                                                                        OOQL.CreateProperty("ITEM_LOT.LOT_CODE", "LOT_CODE"),
                                                                        OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.INVENTORY_QTY", "INVENTORY_QTY"))
                                                                    .From("ITEM_WAREHOUSE_BIN", "ITEM_WAREHOUSE_BIN")
                                                                    .LeftJoin("WAREHOUSE", "WAREHOUSE")
                                                                    .On((OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.WAREHOUSE_ID")))
                                                                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                                                                    .On((OOQL.CreateProperty("BIN.BIN_ID") == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BIN_ID")))
                                                                    .LeftJoin("ITEM_LOT", "ITEM_LOT")
                                                                    .On((OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID") == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_LOT_ID")))
                                                                    .Where(conditionGroup)), "ITEM_WAREHOUSE_BIN")
                                                            .On((OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID"))
                                                                         & (OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID"))
                                                                         & (OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.BO_ID") == OOQL.CreateConstants("OTHER"))
                        //20170104 add by shenbao for P001-161215001 =============begin==============
                                                                         & (Formulas.IsNull(OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"), OOQL.CreateConstants(string.Empty))
                                                                                == OOQL.CreateProperty("ITEM_WAREHOUSE_BIN.LOT_CODE")
                                                                            | Formulas.IsNull(OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"), OOQL.CreateConstants(string.Empty))
                                                                                == OOQL.CreateConstants(string.Empty)
                        //20170104 add by shenbao for P001-161215001 =============end================
                                                                         ));
                }
            }
            return joinOnNode.Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 查询条码类型
        /// </summary>
        /// <param name="barcodeNo">条码</param>
        /// <returns></returns>
        private string GetBarcodeType(string barcodeNo) {
            QueryNode node = OOQL.Select(
                                                        OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "BARCODE_TYPE"))
                                                    .From("BC_RECORD", "BC_RECORD")
                                                    .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
            return this.GetService<IQueryService>().ExecuteScalar(node).ToStringExtension();
        }

        /// <summary>
        /// 查询工厂ID
        /// </summary>
        /// <param name="siteNo">工厂编号</param>
        /// <returns></returns>
        private object GetPlantId(string siteNo) {
            QueryNode node = OOQL.Select(
                                                        OOQL.CreateProperty("PLANT.PLANT_ID"))
                                                    .From("PLANT", "PLANT")
                                                    .Where((OOQL.AuthFilter("PLANT", "PLANT"))
                                                            & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)));
            return this.GetService<IQueryService>().ExecuteScalar(node);
        }

        /// <summary>
        /// 查询是否启用条码库存管理/依单据生成条码
        /// </summary>
        /// <returns></returns>
        private bool GetBcInventoryManagement() {
            QueryNode node = OOQL.Select(
                                                        OOQL.CreateProperty("PARA_FIL.BC_INVENTORY_MANAGEMENT"))
                                                    .From("PARA_FIL", "PARA_FIL")
                                                    .Where(OOQL.AuthFilter("PARA_FIL", "PARA_FIL"));
            return this.GetService<IQueryService>().ExecuteScalar(node).ToBoolean();
        }

        /// <summary>
        /// 2.采购入库
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private QueryNode QueryPurchaseReceipt(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"),//条码批号 //20161229 add by shenbao for P001-161215001
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                //Formulas.Case(null,
                //         OOQL.CreateConstants(1, GeneralDBType.Decimal),
                //         OOQL.CreateCaseArray(
                //                 OOQL.CreateCaseItem(
                //                         (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                //                         Formulas.Cast(
                //                                 OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"),  //20161229 mark by shenbao for P001-161215001
                                    OOQL.CreateConstants(0, "inventory_qty"),  //20161229 add by shenbao for P001-161215001
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("PURCHASE_ARRIVAL.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("PURCHASE_ARRIVAL_D.SequenceNumber"),
                                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:str
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_D", "PURCHASE_ARRIVAL_D")
                                .On((OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("PURCHASE_ARRIVAL", "PURCHASE_ARRIVAL")
                                .On((OOQL.CreateProperty("PURCHASE_ARRIVAL.PURCHASE_ARRIVAL_ID") == OOQL.CreateProperty("PURCHASE_ARRIVAL_D.PURCHASE_ARRIVAL_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 5.销售出货
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QuerySalesShipment(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                                OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                                (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                Formulas.Cast(
                                                        OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_DELIVERY.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_DELIVERY_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("SALES_DELIVERY.SALES_DELIVERY_D", "SALES_DELIVERY_D")
                                .On((OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("SALES_DELIVERY", "SALES_DELIVERY")
                                .On((OOQL.CreateProperty("SALES_DELIVERY.SALES_DELIVERY_ID") == OOQL.CreateProperty("SALES_DELIVERY_D.SALES_DELIVERY_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        #region //20170731 add by liwei1 for P001-170717001
        /// <summary>
        /// 5-1.寄售调拨单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QuerySalesOrderDoc(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                                OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                                (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                Formulas.Cast(
                                                        OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_ORDER_DOC_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_ORDER_DOC_SD.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                                .On((OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_SD_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D", "SALES_ORDER_DOC_D")
                                .On((OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_D_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_D_ID")))
                                .LeftJoin("SALES_ORDER_DOC", "SALES_ORDER_DOC")
                                .On((OOQL.CreateProperty("SALES_ORDER_DOC.SALES_ORDER_DOC_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }
        #endregion

        //20170905 add by wangyq for P001-170717001  =================begin===================
        /// <summary>
        /// 5.销售出货//5-2 copy过来修改
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QuerySalesShipment01(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                                OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                                (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                Formulas.Cast(
                                                        OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_ISSUE.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("SALES_ISSUE_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("SALES_ISSUE.SALES_ISSUE_D", "SALES_ISSUE_D")
                                .On((OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("SALES_ISSUE", "SALES_ISSUE")
                                .On((OOQL.CreateProperty("SALES_ISSUE.SALES_ISSUE_ID") == OOQL.CreateProperty("SALES_ISSUE_D.SALES_ISSUE_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }
        //20170905 add by wangyq for P001-170717001  =================end===================

        /// <summary>
        /// 7.工单发料 AND 【入参status】 =A.新增
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryMoStoreIssueA(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("MO.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("MO_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联表
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("MO.MO_D", "MO_D")
                                .On((OOQL.CreateProperty("MO_D.MO_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("MO", "MO")
                                .On((OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);

        }

        /// <summary>
        /// 7.工单发料 AND 【入参status】 =S.过账
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryMoStoreIssueS(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        /// <summary>
        /// 7-3.领料单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryIssueReceiptReq(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("ISSUE_RECEIPT_REQ.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_D", "ISSUE_RECEIPT_REQ_D")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("ISSUE_RECEIPT_REQ", "ISSUE_RECEIPT_REQ")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT_REQ.ISSUE_RECEIPT_REQ_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_REQ_D.ISSUE_RECEIPT_REQ_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        /// <summary>
        /// 8.工单退料 AND 【入参status】 =A.新增
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private QueryNode QueryMoMaterialReturnA(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"),//条码批号 //20161229 add by shenbao for P001-161215001
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                //Formulas.Case(null,
                //        OOQL.CreateConstants(1, GeneralDBType.Decimal),
                //        OOQL.CreateCaseArray(
                //                OOQL.CreateCaseItem(
                //                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                //                        Formulas.Cast(
                //                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"), //20161229 mark by shenbao for P001-161215001
                OOQL.CreateConstants(0, "inventory_qty"),  //20161229 add by shenbao for P001-161215001
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("MO.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("MO_D.SequenceNumber"),
                                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:str
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("MO.MO_D", "MO_D")
                                .On((OOQL.CreateProperty("MO_D.MO_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("MO", "MO")
                                .On((OOQL.CreateProperty("MO.MO_ID") == OOQL.CreateProperty("MO_D.MO_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));

        }

        /// <summary>
        /// 8.工单退料 AND 【入参status】 =S.过账
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private QueryNode QueryMoMaterialReturnS(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"), //条码批号 //20161229 add by shenbao for P001-161215001
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                //Formulas.Case(null,
                //        OOQL.CreateConstants(1, GeneralDBType.Decimal),
                //        OOQL.CreateCaseArray(
                //                OOQL.CreateCaseItem(
                //                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                //                        Formulas.Cast(
                //                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"),  //20161229 mark by shenbao for P001-161215001
                                    OOQL.CreateConstants(0, "inventory_qty"),  //20161229 add by shenbao for P001-161215001
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("ISSUE_RECEIPT.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("ISSUE_RECEIPT_D.SequenceNumber"),
                                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:str
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))

                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("ISSUE_RECEIPT.ISSUE_RECEIPT_D", "ISSUE_RECEIPT_D")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("ISSUE_RECEIPT", "ISSUE_RECEIPT")
                                .On((OOQL.CreateProperty("ISSUE_RECEIPT.ISSUE_RECEIPT_ID") == OOQL.CreateProperty("ISSUE_RECEIPT_D.ISSUE_RECEIPT_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                    & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 9-2.生产入库单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private QueryNode QueryMoReceiptRequistion(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"), //条码批号 //20161229 add by shenbao for P001-161215001
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                //Formulas.Case(null,
                //         OOQL.CreateConstants(1, GeneralDBType.Decimal),
                //         OOQL.CreateCaseArray(
                //                 OOQL.CreateCaseItem(
                //                         (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                //                         Formulas.Cast(
                //                                 OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"),  //20161229 mark by shenbao for P001-161215001
                OOQL.CreateConstants(0, "inventory_qty"),  //20161229 add by shenbao for P001-161215001
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("MO_RECEIPT_REQUISTION.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("MO_RECEIPT_REQUISTION_D.SequenceNumber"),
                                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:str
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("MO_RECEIPT_REQUISTION.MO_RECEIPT_REQUISTION_D", "MO_RECEIPT_REQUISTION_D")
                                .On((OOQL.CreateProperty("MO_RECEIPT_REQUISTION_D.MO_RECEIPT_REQUISTION_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("MO_RECEIPT_REQUISTION", "MO_RECEIPT_REQUISTION")
                                .On((OOQL.CreateProperty("MO_RECEIPT_REQUISTION.MO_RECEIPT_REQUISTION_ID") == OOQL.CreateProperty("MO_RECEIPT_REQUISTION_D.MO_RECEIPT_REQUISTION_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        //20170801 add by shenbao for P001-170717001
        /// <summary>
        /// 9-3.生产入库工单
        /// </summary>
        private QueryNode QueryMoReceiptMo(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"), //条码批号 
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                                    OOQL.CreateConstants(0, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("MO.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("MO")
                                .On((OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid") == OOQL.CreateProperty("MO.MO_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 11.杂项发料
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="paraFilEntity"></param>
        /// <returns></returns>
        private QueryNode QueryMiscellaneousIssues(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));
            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));
            //组合关联条件
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        /// <summary>
        /// 12.杂项收料
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="paraFilEntity"></param>
        /// <returns></returns>
        private QueryNode QueryMiscellaneousCharge(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no")); //条码批号 //20161229 add by shenbao for P001-161215001
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            //propertyList.Add(Formulas.Case(null,
            //        OOQL.CreateConstants(1, GeneralDBType.Decimal),
            //        OOQL.CreateCaseArray(
            //                OOQL.CreateCaseItem(
            //                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
            //                        Formulas.Cast(
            //                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"));  //20161229 mark by shenbao for P001-161215001
            propertyList.Add(OOQL.CreateConstants(0, "inventory_qty"));  //20161229 add by shenbao for P001-161215001
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("TRANSACTION_DOC.DOC_NO"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("TRANSACTION_DOC_D.SequenceNumber"),
                    OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));

            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));
            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合查询字段和关联表
            return OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .LeftJoin("TRANSACTION_DOC.TRANSACTION_DOC_D", "TRANSACTION_DOC_D")
                                .On((OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("TRANSACTION_DOC", "TRANSACTION_DOC")
                                .On((OOQL.CreateProperty("TRANSACTION_DOC.TRANSACTION_DOC_ID") == OOQL.CreateProperty("TRANSACTION_DOC_D.TRANSACTION_DOC_ID")))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                     & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 13-1.调拨单 OR 13-2.调出单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryTransferRequisitionDoc(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            //启用条码库存管理
            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "source_no"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        /// <summary>
        /// 13-3.拨入单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="plantId"></param>
        /// <returns></returns>
        private QueryNode QueryDialInOrder(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"));
            propertyList.Add(OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(str, GeneralDBType.String),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no")); //条码批号 //20161229 add by shenbao for P001-161215001
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            //propertyList.Add(Formulas.Case(null,
            //        OOQL.CreateConstants(1, GeneralDBType.Decimal),
            //        OOQL.CreateCaseArray(
            //                OOQL.CreateCaseItem(
            //                        (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
            //                        Formulas.Cast(
            //                                OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "inventory_qty"));  //20161229 mark by shenbao for P001-161215001
            propertyList.Add(OOQL.CreateConstants(0, "inventory_qty"));  //20161229 add by shenbao for P001-161215001

            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("TRANSFER_DOC.DOC_NO"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("TRANSFER_DOC_D.SequenceNumber"),
                    OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));

            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            return OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("TRANSFER_DOC.TRANSFER_DOC_D", "TRANSFER_DOC_D")
                                .On((OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("TRANSFER_DOC", "TRANSFER_DOC")
                                .On((OOQL.CreateProperty("TRANSFER_DOC.TRANSFER_DOC_ID") == OOQL.CreateProperty("TRANSFER_DOC_D.TRANSFER_DOC_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                     & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 13-5.调拨单
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryTransferRequisition(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                    OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                    OOQL.CreateCaseArray(
                            OOQL.CreateCaseItem(
                                    (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                    Formulas.Cast(
                                            OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("TRANSFER_REQUISITION.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("TRANSFER_REQUISITION_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("TRANSFER_REQUISITION.TRANSFER_REQUISITION_D", "TRANSFER_REQUISITION_D")
                                .On((OOQL.CreateProperty("TRANSFER_REQUISITION_D.TRANSFER_REQUISITION_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("TRANSFER_REQUISITION", "TRANSFER_REQUISITION")
                                .On((OOQL.CreateProperty("TRANSFER_REQUISITION.TRANSFER_REQUISITION_ID") == OOQL.CreateProperty("TRANSFER_REQUISITION_D.TRANSFER_REQUISITION_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            //组合条件
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }

        #region 20170215 add by liwei1 for P001-170203001
        /// <summary>
        /// 6.销退单 
        /// </summary>
        /// <param name="site_no"></param>
        /// <param name="program_job_no"></param>
        /// <param name="barcode_no"></param>
        /// <param name="warehouse_no"></param>
        /// <param name="storage_spaces_no"></param>
        /// <param name="lot_no"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QuerySalesReturn(string siteNo, string programJobNo, string barcodeNo, object plantId) {
            string str = string.Empty;
            return OOQL.Select(true,
                                    OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"),
                                    OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"),
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"),
                                    OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"),
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "warehouse_no"),
                                    OOQL.CreateConstants(str, GeneralDBType.String, "storage_spaces_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "lot_no"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(str, GeneralDBType.String),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            OOQL.CreateProperty("SYS_ITEM_LOT.BC_PROPERTY_VALUE"))), "barcode_lot_no"),//条码批号 //20161229 add by shenbao for P001-161215001
                                    Formulas.IsNull(
                                            OOQL.CreateProperty("UNIT.UNIT_CODE"),
                                            OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"),
                                    Formulas.Case(null,
                                            OOQL.CreateConstants(1, GeneralDBType.Decimal),
                                            OOQL.CreateCaseArray(
                                                    OOQL.CreateCaseItem(
                                                            (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                            Formulas.Cast(
                                                                    OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"),
                                    OOQL.CreateConstants(0, "inventory_qty"),
                                    OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("SALES_RETURN.DOC_NO"),
                                        OOQL.CreateConstants(str, GeneralDBType.String), "source_no"),
                                    Formulas.IsNull(
                                        OOQL.CreateProperty("SALES_RETURN_D.SequenceNumber"),
                                        OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"),
                                    OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"),
                                    OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"),
                                    OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"),//20170328 modi by wangyq for P001-170327001 old:str
                                    OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"),
                                    OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"),
                                    OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"),
                                    Formulas.Case(null,
                                        OOQL.CreateConstants("1", GeneralDBType.String),
                                        OOQL.CreateCaseArray(
                                                OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                                        OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"))
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_Item_Lot", GeneralDBType.String))), "SYS_ITEM_LOT")
                                .On((OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID") == OOQL.CreateProperty("SYS_ITEM_LOT.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("SALES_RETURN.SALES_RETURN_D", "SALES_RETURN_D")
                                .On((OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("SALES_RETURN", "SALES_RETURN")
                                .On((OOQL.CreateProperty("SALES_RETURN.SALES_RETURN_ID") == OOQL.CreateProperty("SALES_RETURN_D.SALES_RETURN_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)))
                                .Where((OOQL.AuthFilter("BC_RECORD", "BC_RECORD"))
                                            & (OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)));
        }

        /// <summary>
        /// 4.采购退货
        /// </summary>
        /// <param name="siteNo"></param>
        /// <param name="programJobNo"></param>
        /// <param name="barcodeNo"></param>
        /// <param name="warehouseNo"></param>
        /// <param name="storageSpacesNo"></param>
        /// <param name="lotNo"></param>
        /// <param name="plantId"></param>
        /// <param name="bcInventoryManagement"></param>
        /// <returns></returns>
        private QueryNode QueryPurchaseReturn(string siteNo, string programJobNo, string barcodeNo, string warehouseNo,
            string storageSpacesNo, string lotNo, object plantId, bool bcInventoryManagement) {
            string str = string.Empty;
            //组合查询字段
            List<QueryProperty> propertyList = new List<QueryProperty>();
            propertyList.Add(OOQL.CreateConstants("99", GeneralDBType.String, "enterprise_no"));
            propertyList.Add(OOQL.CreateConstants(siteNo, GeneralDBType.String, "site_no"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_NO", "barcode_no"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_CODE", "item_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_no"));
            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("ITEM_FEATURE.ITEM_SPECIFICATION"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "item_feature_name"));

            //条码类型
            string barcodeType = GetBarcodeType(barcodeNo);
            AddInventoryQty(propertyList, barcodeType, bcInventoryManagement);

            propertyList.Add(Formulas.IsNull(
                    OOQL.CreateProperty("UNIT.UNIT_CODE"),
                    OOQL.CreateConstants(str, GeneralDBType.String), "inventory_unit"));
            propertyList.Add(Formulas.Case(null,
                                OOQL.CreateConstants(1, GeneralDBType.Decimal),//20170809 modi by liwei1 for P001-170717001 默认是应该是1
                                OOQL.CreateCaseArray(
                                        OOQL.CreateCaseItem(
                                                (OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE") != OOQL.CreateConstants(str, GeneralDBType.String)),
                                                Formulas.Cast(
                                                        OOQL.CreateProperty("SYS_INV_QTY.BC_PROPERTY_VALUE"), GeneralDBType.Decimal, 20, 8))), "barcode_qty"));
            propertyList.Add(OOQL.CreateConstants(programJobNo, GeneralDBType.String, "source_operation"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("PURCHASE_RETURN.DOC_NO"),
                OOQL.CreateConstants(str, GeneralDBType.String), "source_no"));
            propertyList.Add(Formulas.IsNull(
                OOQL.CreateProperty("PURCHASE_RETURN_D.SequenceNumber"),
                OOQL.CreateConstants(0, GeneralDBType.Int32), "source_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_line_seq"));
            propertyList.Add(OOQL.CreateConstants(0, GeneralDBType.Int32, "source_batch_seq"));
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.LastModifiedDate", "last_transaction_date"));

            propertyList.Add(OOQL.CreateConstants(UtilsClass.SpaceValue, GeneralDBType.String, "inventory_management_features"));//20170328 modi by wangyq for P001-170327001 old:str
            propertyList.Add(OOQL.CreateProperty("BC_RECORD.BARCODE_TYPE", "barcode_type"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_NAME", "item_name"));
            propertyList.Add(OOQL.CreateProperty("ITEM.ITEM_SPECIFICATION", "item_spec"));
            propertyList.Add(Formulas.Case(null,
                OOQL.CreateConstants("1", GeneralDBType.String),
                OOQL.CreateCaseArray(
                        OOQL.CreateCaseItem((OOQL.CreateProperty("ITEM_PLANT.LOT_CONTROL") == OOQL.CreateConstants("N", GeneralDBType.String)),
                                OOQL.CreateConstants("2", GeneralDBType.String))), "lot_control_type"));

            //组合关联
            JoinOnNode joinOnNode = OOQL.Select(true, propertyList.ToArray())
                                .From("BC_RECORD", "BC_RECORD")
                                .InnerJoin("BC_RECORD.BC_RECORD_D", "BC_RECORD_D")
                                .On((OOQL.CreateProperty("BC_RECORD_D.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .LeftJoin(
                                    OOQL.Select(
                                            OOQL.CreateProperty("A.BC_PROPERTY_VALUE"),
                                            OOQL.CreateProperty("A.BC_RECORD_ID"))
                                        .From("BC_RECORD.BC_RECORD_D", "A")
                                        .LeftJoin("BC_PROPERTY", "B")
                                        .On((OOQL.CreateProperty("B.BC_PROPERTY_ID") == OOQL.CreateProperty("A.BC_PROPERTY_ID")))
                                        .Where((OOQL.CreateProperty("B.BC_PROPERTY_CODE") == OOQL.CreateConstants("Sys_INV_Qty", GeneralDBType.String))), "SYS_INV_QTY")
                                .On((OOQL.CreateProperty("SYS_INV_QTY.BC_RECORD_ID") == OOQL.CreateProperty("BC_RECORD.BC_RECORD_ID")))
                                .InnerJoin("ITEM", "ITEM")
                                .On((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_ID")))
                                .LeftJoin("UNIT", "UNIT")
                                .On((OOQL.CreateProperty("UNIT.UNIT_ID") == OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")))
                                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                                .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("BC_RECORD.ITEM_FEATURE_ID")))
                                .LeftJoin("PURCHASE_RETURN.PURCHASE_RETURN_D", "PURCHASE_RETURN_D")
                                .On((OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_D_ID") == OOQL.CreateProperty("BC_RECORD.SOURCE_D_ID.ROid")))
                                .LeftJoin("PURCHASE_RETURN", "PURCHASE_RETURN")
                                .On((OOQL.CreateProperty("PURCHASE_RETURN.PURCHASE_RETURN_ID") == OOQL.CreateProperty("PURCHASE_RETURN_D.PURCHASE_RETURN_ID")))
                                .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(plantId)));

            //根据是否启用条码库存管理动态关联
            return AddJoinOnNode(joinOnNode, barcodeType, bcInventoryManagement, warehouseNo, storageSpacesNo, lotNo, barcodeNo);
        }
        #endregion

        //20170328 add by wangyq for P001-170327001  从GetDocAndBarcodeService的1||3的条件下QueryBarCode直接copy过滤=============begin=============
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
        private QueryNode GetBcRecord(string barcodeNo, string siteNo, string programJobNo, object objQueryQtyBcPropertyId, object objQueryItemLotBcPropertyId) {
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
                            Formulas.Cast(OOQL.CreateConstants(UtilsClass.SpaceValue), GeneralDBType.String, 10, "inventory_management_features"), //库存管理特征//20170328 modi by wangyq for P001-170327001 old:""
                            OOQL.CreateProperty("UNIT.UNIT_CODE", "inventory_unit"), //库存单位
                            Formulas.Cast(Formulas.Case(null, OOQL.CreateConstants("1"),
                                new CaseItem[]{
                                    new CaseItem(Formulas.IsNull(OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"),OOQL.CreateConstants("")) != OOQL.CreateConstants(""),
                                        OOQL.CreateProperty("BC_RECORD_D.BC_PROPERTY_VALUE"))
                                },
                                string.Empty), GeneralDBType.Decimal, 20, 8, "barcode_qty"),//条码数量
                            Formulas.Cast(OOQL.CreateConstants(0M), GeneralDBType.Decimal, 16, 6, "inventory_qty"), //库存数量
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
                    .InnerJoin("ITEM_PLANT", "ITEM_PLANT")//20170504 modi by wangyq for P001-170427001 old:LeftJoin
                    .On(OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID") &
                        OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                    .Where(OOQL.CreateProperty("BC_RECORD.BARCODE_NO") == OOQL.CreateConstants(barcodeNo)
                //&  OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)//20170328 mark by wangyq for P001-170327001
                           );

            return node;
        }

        /// <summary>
        /// 查询条码规则属性项
        /// </summary>
        /// <param name="queryService">查询服务</param>
        /// <param name="bcPropertyCode">属性项编码</param>
        /// <returns></returns>
        private object GetBcPropertyId(string bcPropertyCode) {
            QueryNode node =
                OOQL.Select(OOQL.CreateProperty("BC_PROPERTY_ID"))
                    .From("BC_PROPERTY")
                    .Where(OOQL.CreateProperty("BC_PROPERTY_CODE") == OOQL.CreateConstants(bcPropertyCode));

            object result = this.GetService<IQueryService>().ExecuteScalar(node);  //20161229 add by shenbao for P001-161215001

            return result;
        }
        //20170328 add by wangyq for P001-170327001  从GetDocAndBarcodeService的1||3的条件下QueryBarCode直接copy过滤=============end=============
        #endregion
    }
}
