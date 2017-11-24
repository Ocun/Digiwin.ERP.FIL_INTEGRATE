//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>liwei1</author>
//<createDate>2017/07/20</createDate>
//<IssueNo>P001-170717001</IssueNo>
//<description>产生寄售调拨单服务</description>


using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.ERP.EFNET.Business;
using Digiwin.ERP.CommonSupplyChain.Business;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IInsertConsignTransferOutService))]
    [Description("产生寄售调拨单服务实现")]
    public sealed class InsertConsignTransferOutService : ServiceComponent, IInsertConsignTransferOutService {
        #region 相关服务

        private IInfoEncodeContainer _encodeSrv;
        /// <summary>
        /// 信息编码服务
        /// </summary>
        public IInfoEncodeContainer EncodeSrv {
            get { return _encodeSrv ?? (_encodeSrv = GetService<IInfoEncodeContainer>()); }
        }

        private IPrimaryKeyService _primaryKeySrv;
        /// <summary>
        /// 主键生成服务
        /// </summary>
        public IPrimaryKeyService PrimaryKeySrv {
            get { return _primaryKeySrv ?? (_primaryKeySrv = GetService<IPrimaryKeyService>("CONSIGN_TRANSFER_OUT")); }
        }

        private IDocumentNumberGenerateService _documentNumberGenSrv;
        /// <summary>
        /// 生成单号服务
        /// </summary>
        public IDocumentNumberGenerateService DocumentNumberGenSrv {
            get {
                return _documentNumberGenSrv ??
                       (_documentNumberGenSrv = GetService<IDocumentNumberGenerateService>("CONSIGN_TRANSFER_OUT"));
            }
        }

        #endregion

        #region 自定义字段

        /// <summary>
        /// 工厂对应的单据类型信息
        /// </summary>
        private DependencyObjectCollection _docInfos;

        /// <summary>
        /// 记录每个单据类型对应的当前单号
        /// </summary>
        private Hashtable _currentDocNo = new Hashtable();

        #endregion

        #region 接口方法实现

        /// <summary>
        ///  产生寄售调拨单信息
        /// </summary>
        /// <param name="employeeNo">扫描人员</param>
        /// <param name="scanType">扫描类型 1.有箱条码 2.无箱条码</param>
        /// <param name="reportDatetime">上传时间</param>
        /// <param name="pickingDepartmentNo">领料部门</param>
        /// <param name="recommendedOperations">建议执行作业</param>
        /// <param name="recommendedFunction">A.新增  S.过帐</param>
        /// <param name="scanDocNo">扫描单号</param>
        /// <param name="collection">接口传入的领料单单身数据集合</param>
        public DependencyObjectCollection InsertConsignTransferOut(string employeeNo, string scanType, DateTime reportDatetime, string pickingDepartmentNo
            , string recommendedOperations, string recommendedFunction, string scanDocNo, DependencyObjectCollection collection) {
            DependencyObjectCollection rtnColl = CreateReturnCollection();
            #region 参数检查
            if (Maths.IsEmpty(recommendedOperations)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_operations" }));
            }
            if (Maths.IsEmpty(recommendedFunction)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "recommended_function" }));
            }
            //没有设置默认的寄售仓，请设置！（A111564）
            if (collection.Count > 0 && GetConsigeCount(collection[0]["info_lot_no"].ToStringExtension(), collection[0]["site_no"].ToStringExtension()) == 0) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111564"));
            }
            #endregion

            //创建临时表需要的DataTable和Mapping信息
            DataTable consignTransferOut = null, consignTransferOutD = null;
            List<BulkCopyColumnMapping> consignTransferOutMap = null, consignTransferOutDMap = null;
            CreateRelateTable(ref consignTransferOut, ref consignTransferOutD,ref consignTransferOutMap, ref consignTransferOutDMap);

            List<DocumentInfo> listDocuments = new List<DocumentInfo>();  //记录生成的单据信息

            //组织数据BulkCopy需要的DataTable数据
            GetDocInfo(collection);
            //E+MSG：A111275(未找到对应的FIL单据类型设置，请检查！)
            if (_docInfos.Count <= 0) throw new BusinessRuleException(EncodeSrv.GetMessage("A111275"));
            //将传过来参数暂存至consignTransferOut和consignTransferOutD中，用于后面一次性插入临时表
            InsertDataTable(consignTransferOut, consignTransferOutD, collection, listDocuments, employeeNo, pickingDepartmentNo);
            //没有数据值不再往下执行
            if (consignTransferOut.Rows.Count <= 00 || consignTransferOutD.Rows.Count <= 0)  return rtnColl;

            #region 新增逻辑
            using (ITransactionService trans = GetService<ITransactionService>()) {
                IQueryService querySrv = GetService<IQueryService>();

                //新增临时表
                IDataEntityType consignTransferOutTmp = CreateConsignTransferOutTmpTable(querySrv);
                IDataEntityType consignTransferOutDTmp = CreateConsignTransferOutDTmpTable(querySrv);

                //批量新增到临时表
                querySrv.BulkCopy(consignTransferOut, consignTransferOutTmp.Name, consignTransferOutMap.ToArray());
                querySrv.BulkCopy(consignTransferOutD, consignTransferOutDTmp.Name, consignTransferOutDMap.ToArray());

                //利用临时表批量新增相关数据
                InsertConsignTransferOut(querySrv, consignTransferOutTmp, reportDatetime);
                InsertConsignTransferOutD(querySrv, consignTransferOutDTmp);
                InsertBcLine(querySrv, consignTransferOutDTmp);

                //修改单身/单头汇总字段
                UpdateConsignTransferOut(querySrv, listDocuments.Select(c => c.Id).ToArray());
                
                //EFNET签核
                var infos = listDocuments.GroupBy(c => new { DOC_ID = c.DocId, OwnerOrgID = c.OwnerOrgId });
                foreach (var item in infos) {
                    IEFNETStatusStatusService efnetSrv = GetService<IEFNETStatusStatusService>();
                    efnetSrv.GetFormFlow("CONSIGN_TRANSFER_OUT.I01", item.Key.DOC_ID, item.Key.OwnerOrgID,
                         item.Select(c => c.Id).ToArray());
                }

                //保存单据
                IReadService readSrv = GetService<IReadService>("CONSIGN_TRANSFER_OUT");
                object[] entities = readSrv.Read(listDocuments.Select(c => c.Id).Distinct().ToArray());
                if (entities != null && entities.Length > 0) {
                    ISaveService saveSrv = GetService<ISaveService>("CONSIGN_TRANSFER_OUT");
                    saveSrv.Save(entities);
                }

                trans.Complete();
            }
            #endregion

            #region 组织返回结果

            foreach (DocumentInfo item in listDocuments) {
                DependencyObject obj = rtnColl.AddNew();
                obj["doc_no"] = item.DocNo;
            }

            #endregion

            #region 释放内存
            ClearData();
            #endregion

            return rtnColl;
        }

        #endregion

        #region 自定义方法

        /// <summary>
        /// 查询：寄售订单信息（没有设置默认的寄售仓，请设置！）
        /// </summary>
        /// <param name="docNo">单号</param>
        /// <returns></returns>
        private int GetConsigeCount(string docNo,string siteNo){
            QueryNode node = 
                OOQL.Select(
                            Formulas.Count(
                                OOQL.CreateProperty("CONSIGN.CONSIGN_ID"), "COUNT_NMB"))
                        .From("SALES_ORDER_DOC", "SALES_ORDER_DOC")
                        .InnerJoin("CONSIGN", "CONSIGN")
                        .On((OOQL.CreateProperty("CONSIGN.CUSTOMER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID"))
                            & (OOQL.CreateProperty("CONSIGN.SALES_CENTER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid"))
                            & (OOQL.CreateProperty("CONSIGN.PLANT_ID") == OOQL.Select(1,OOQL.CreateProperty("PLANT.PLANT_ID"))
                                .From("PLANT", "PLANT")
                                .Where((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(siteNo)))))
                        .Where((OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO") == OOQL.CreateConstants(docNo)));
            return GetService<IQueryService>().ExecuteScalar(node).ToInt32();
        }

        /// <summary>
        /// 创建服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("doc_no", typeof(string));
            DependencyObjectCollection rtn = new DependencyObjectCollection(type);
            return rtn;
        }

        /// <summary>
        /// 获取每笔记录对应的单据类型
        /// 用于后面生成单号使用
        /// 后面以工厂来取得，因为一个工厂对于相同的Catagory在FIL单据类型设置只可能有一个单据类型
        /// </summary>
        /// <param name="collection"></param>
        /// <returns></returns>
        private void GetDocInfo( DependencyObjectCollection collection){
            if (collection.Count == 0) return;
            QueryNode node = OOQL.Select(true
                    , "PLANT.PLANT_CODE"
                    , "PLANT.PLANT_ID"
                    , "PLANT.COMPANY_ID"
                    , "PARA_DOC_FIL.DOC_ID"
                    , "DOC.SEQUENCE_DIGIT"
                    , "PARA_DOC_FIL.SOURCE_DOC_ID"
                )
                .From("PLANT", "PLANT")
                .InnerJoin("PARA_DOC_FIL")
                .On(OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID")
                    & OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants("2C"))
                .Where(OOQL.AuthFilter("PLANT", "PLANT")
                       & ((OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(collection[0]["site_no"]))
                          & ((OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                             | (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.Select(1, OOQL.CreateProperty("DOC_ID"))
                                 .From("SALES_ORDER_DOC")
                                 .Where((OOQL.CreateProperty("DOC_NO") ==OOQL.CreateConstants(collection[0]["info_lot_no"])))))))
                .OrderBy(
                    OOQL.CreateOrderByItem(
                        OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID"), SortType.Desc));

            _docInfos = GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        /// <summary>
        /// 组织更新consignTransferOut consignTransferOutD DataTable
        /// </summary>
        /// <param name="consignTransferOut"></param>
        /// <param name="consignTransferOutD"></param>
        /// <param name="colls"></param>
        /// <param name="documents"></param>
        /// <param name="employeeNo"></param>
        /// <param name="pickingDepartmentNo"></param>
        private void InsertDataTable(DataTable consignTransferOut, DataTable consignTransferOutD
            , DependencyObjectCollection colls, List<DocumentInfo> documents, string employeeNo, string pickingDepartmentNo) {
            foreach (DependencyObject obj in colls) {
                #region 新增表consignTransferOut结构
                object key = PrimaryKeySrv.CreateId();  //主键
                string docNo;
                DependencyObject infoItem = _docInfos.FirstOrDefault(c => c["PLANT_CODE"].ToStringExtension() == obj["site_no"].ToStringExtension());
                if (infoItem == null) break;

                DocumentInfo doc = new DocumentInfo();
                //计算单号
                if (!_currentDocNo.ContainsKey(infoItem["DOC_ID"])) {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, "", infoItem["DOC_ID"], infoItem["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);
                    _currentDocNo.Add(infoItem["DOC_ID"], docNo);
                } else {
                    docNo = UtilsClass.NextNumber(DocumentNumberGenSrv, _currentDocNo[infoItem["DOC_ID"]].ToStringExtension(), infoItem["DOC_ID"], infoItem["SEQUENCE_DIGIT"].ToInt32(), DateTime.Now);
                    _currentDocNo[infoItem["DOC_ID"]] = docNo;
                }

                //组织表结构
                DataRow dr = consignTransferOut.NewRow();
                dr["ID"] = key;  //主键
                dr["doc_id"] = infoItem["DOC_ID"];  //单据类型
                dr["doc_no"] = docNo;  //单号
                dr["employee_no"] = employeeNo;  //人员
                dr["picking_department_no"] = pickingDepartmentNo;  //部门
                dr["site_no"] = obj["site_no"]; //工厂编号
                dr["info_lot_no"] = obj["info_lot_no"];  //信息批号
                dr["company_id"] = infoItem["COMPANY_ID"];  //工厂对应的公司ID

                //记录单据信息
                doc.Id = key;
                doc.DocId = infoItem["DOC_ID"];
                doc.OwnerOrgId = infoItem["PLANT_ID"];
                doc.DocNo = docNo;
                documents.Add(doc);

                //添加数据至对应表中
                consignTransferOut.Rows.Add(dr);

                #endregion

                DependencyObjectCollection subColls = obj["scan_detail"] as DependencyObjectCollection;
                if (subColls != null && subColls.Count > 0) {
                    //序号
                    int sequenceno = 1;
                    //根据唯一性字段来记录对应的行信息
                    Dictionary<string, EntityLine> lineKeyDic = new Dictionary<string, EntityLine>(); 
                    foreach (DependencyObject subObj in subColls) {
                        //唯一性字段组合：源单单号+序号+品号+特征码+仓库+库位+批号
                        string uniqueKey = string.Concat(subObj["doc_no"].ToStringExtension(), subObj["seq"].ToStringExtension()
                        , subObj["item_no"].ToStringExtension(), subObj["item_feature_no"].ToStringExtension(), subObj["warehouse_no"].ToStringExtension()
                        , subObj["storage_spaces_no"].ToStringExtension(), subObj["lot_no"].ToStringExtension());
                        #region 新增表consignTransferOutD结构

                        //组织DataTable
                        dr = consignTransferOutD.NewRow();
                        dr["CONSIGN_TRANSFER_OUT_ID"] = key;  //父主键
                        if (!lineKeyDic.ContainsKey(uniqueKey)) {  //新的一组，重新生成行主键和行号
                            EntityLine line = new EntityLine();
                            line.UniqueKey = uniqueKey;
                            line.Key = PrimaryKeySrv.CreateId();
                            line.SequenceNumber = sequenceno++;

                            dr["CONSIGN_TRANSFER_OUT_D_ID"] = line.Key;
                            dr["SequenceNumber"] = line.SequenceNumber;

                            lineKeyDic.Add(uniqueKey, line);
                        } else {  //已经存在的
                            dr["CONSIGN_TRANSFER_OUT_D_ID"] = lineKeyDic[uniqueKey].Key;
                            dr["SequenceNumber"] = lineKeyDic[uniqueKey].SequenceNumber;
                        }
                        dr["info_lot_no"] = subObj["info_lot_no"];  //信息批号
                        dr["item_no"] = subObj["item_no"];  //品号
                        dr["item_feature_no"] = subObj["item_feature_no"];  //特征码
                        dr["picking_unit_no"] = subObj["picking_unit_no"];  //单位
                        dr["doc_no"] = subObj["doc_no"];  //单号
                        dr["seq"] = subObj["seq"].ToInt32();  //来源序号
                        dr["warehouse_no"] = subObj["warehouse_no"];  //仓库编号
                        dr["storage_spaces_no"] = subObj["storage_spaces_no"];  //库位编号
                        dr["lot_no"] = subObj["lot_no"];  //批号
                        dr["picking_qty"] = subObj["picking_qty"].ToDecimal();  //拣货数量
                        dr["barcode_no"] = subObj["barcode_no"];  //条码编号
                        dr["site_no"] = subObj["site_no"];  //工厂编码
                        dr["doc_id"] = infoItem["DOC_ID"];  //单据类型

                        consignTransferOutD.Rows.Add(dr);
                        #endregion
                    }
                }
            }
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增寄售调拨单
        /// </summary>
        private void InsertConsignTransferOut(IQueryService qrySrv, IDataEntityType tmpConsignTransferOut, DateTime reportDatetime) {
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.ID","CONSIGN_TRANSFER_OUT_ID"),  //主键
                OOQL.CreateConstants("PLANT", "Owner_Org.RTK"),  //组织
                OOQL.CreateProperty("PLANT.PLANT_ID", "Owner_Org.ROid"),  //组织ID
                OOQL.CreateProperty("tmpTable.doc_id","DOC_ID"),  //单据类型
                OOQL.CreateConstants(reportDatetime,"DOC_DATE"),  //单据日期
                OOQL.CreateProperty("tmpTable.doc_no","DOC_NO"),  //单号
                Formulas.IsNull(OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Dept"),  //部门
                Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Emp"),  //人员
                OOQL.CreateConstants(reportDatetime,"TRANSACTION_DATE"),  //调拨日期
                OOQL.CreateConstants("2C","CATEGORY"),  //单据性质码
                Formulas.Cast(OOQL.CreateConstants(0),GeneralDBType.Decimal,"AMT_UNINCLUDE_TAX_BC"),//本币未税金额
                Formulas.Cast(OOQL.CreateConstants(0),GeneralDBType.Decimal,"AMT_UNINCLUDE_TAX_OC"),//原币未税金额
                Formulas.Cast(OOQL.CreateConstants(0),GeneralDBType.Decimal,"TAX_BC"),//本币税额
                Formulas.Cast(OOQL.CreateConstants(0),GeneralDBType.Decimal,"TAX_OC"),//原币税额
                OOQL.CreateProperty("SALES_ORDER_DOC.EXCHANGE_RATE","EXCHANGE_RATE"),//汇率
                OOQL.CreateProperty("SALES_ORDER_DOC.TAX_INCLUDED","TAX_INCLUDED"),//含税
                OOQL.CreateProperty("SALES_ORDER_DOC.REMARK1","REMARK"),//备注
                OOQL.CreateConstants(0,"PIECES"),//件数
                Formulas.Case(null,
				    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.SHIP_TO_ADDR_NAME"), 
                        OOQL.CreateConstants(string.Empty)),
				    OOQL.CreateCaseArray(
					    OOQL.CreateCaseItem((OOQL.CreateProperty("SALES_ORDER_DOC.MULTI_DELIVERY") == OOQL.CreateConstants(false)),
                        OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_ADDR_NAME"))),
                    "SHIP_TO_ADDR_NAME"),//收货地址名称
                Formulas.Case(null,
				    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.SHIP_TO_CONTACT_NAME"), 
                        OOQL.CreateConstants(string.Empty)),
				    OOQL.CreateCaseArray(
					    OOQL.CreateCaseItem((OOQL.CreateProperty("SALES_ORDER_DOC.MULTI_DELIVERY") == OOQL.CreateConstants(false)),
                        OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CONTACT_NAME"))),
                    "SHIP_TO_CONTACT_NAME"),//收货联系人名称
                OOQL.CreateProperty("SALES_ORDER_DOC.CUSTOMER_ID","CUSTOMER_ID"),//客户
                OOQL.CreateProperty("PLANT.COMPANY_ID","COMPANY_ID"),//公司
                OOQL.CreateProperty("SALES_ORDER_DOC.CURRENCY_ID","CURRENCY_ID"),//币种
                OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid","SALES_CENTER_ID"),//销售域
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"WAREHOUSE_ID"),//限用仓库编号
                Formulas.Case(null,
				    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.DELIVERY_TERM_ID"), 
                        OOQL.CreateConstants(Maths.GuidDefaultValue())),
				    OOQL.CreateCaseArray(
					    OOQL.CreateCaseItem((OOQL.CreateProperty("SALES_ORDER_DOC.MULTI_DELIVERY") == OOQL.CreateConstants(false)),
                        OOQL.CreateProperty("SALES_ORDER_DOC.DELIVERY_TERM_ID"))),
                    "DELIVERY_TERM_ID"),//运输方式
                Formulas.Case(null,
				    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.SHIP_TO_ADDR_ID"), 
                        OOQL.CreateConstants(Maths.GuidDefaultValue())),
				    OOQL.CreateCaseArray(
					    OOQL.CreateCaseItem((OOQL.CreateProperty("SALES_ORDER_DOC.MULTI_DELIVERY") == OOQL.CreateConstants(false)),
                        OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_ADDR_ID"))),
                    "SHIP_TO_ADDR_ID"),//收货地址
                Formulas.Case(null,
				    Formulas.IsNull(
                        OOQL.CreateProperty("SALES_ORDER_DOC_SD.SHIP_TO_CONTACT_ID"), 
                        OOQL.CreateConstants(Maths.GuidDefaultValue())),
				    OOQL.CreateCaseArray(
					    OOQL.CreateCaseItem((OOQL.CreateProperty("SALES_ORDER_DOC.MULTI_DELIVERY") == OOQL.CreateConstants(false)),
                        OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CONTACT_ID"))),
                    "SHIP_TO_CONTACT_ID"),//收货联系人
                OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID","SHIP_TO_CUSTOMER_ID"),//收货客户
                Formulas.IsNull(OOQL.CreateProperty("CUSTOMER_SALES.SIGN_REQUIRED"),
                    OOQL.CreateConstants(string.Empty),"SIGN_REQUIRED"),//需签收
                OOQL.CreateConstants(string.Empty,"ACCOUNT_YEAR"),//会计年度
                OOQL.CreateConstants(0,"ACCOUNT_PERIOD_SEQNO"),//会计期间序号
                OOQL.CreateConstants(string.Empty,"ACCOUNT_PERIOD_CODE"),//会计期间期号
                Formulas.IsNull(OOQL.CreateProperty("CONSIGN.WAREHOUSE_ID"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()),"CONSIGN_WAREHOUSE_ID"),//寄售仓
                OOQL.CreateConstants(false,"GLMB_JE_INDICATOR"),//已生成管理账簿分录
                OOQL.CreateConstants(false,"GLOB_JE_INDICATOR"),//已生成运营账簿分录
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"GLMB_JE_ID"),//管理账簿分录
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"GLOB_JE_ID"),//运营账簿分录
            });
               
                
            #endregion

            QueryNode insertNode = OOQL.Select(true,
                properties
                )
                .From(tmpConsignTransferOut.Name, "tmpTable")
                .LeftJoin("EMPLOYEE", "EMPLOYEE")
                .On(OOQL.CreateProperty("tmpTable.employee_no") == OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE"))
                .LeftJoin("ADMIN_UNIT", "ADMIN_UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_department_no") ==OOQL.CreateProperty("ADMIN_UNIT.ADMIN_UNIT_CODE"))
                .LeftJoin("PLANT", "PLANT")
                .On(OOQL.CreateProperty("tmpTable.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .LeftJoin("SALES_ORDER_DOC", "SALES_ORDER_DOC")
                .On(OOQL.CreateProperty("tmpTable.info_lot_no") == OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO"))
                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D", "SALES_ORDER_DOC_D")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.SALES_ORDER_DOC_ID"))
                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_D_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_D_ID")
                    & OOQL.CreateProperty("SALES_ORDER_DOC_SD.SequenceNumber") == OOQL.CreateConstants(1))                
                .LeftJoin("CUSTOMER_SALES", "CUSTOMER_SALES")
                .On(OOQL.CreateProperty("CUSTOMER_SALES.CUSTOMER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID")
                    &OOQL.CreateProperty("CUSTOMER_SALES.Owner_Org.ROid") ==OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid"))
                .LeftJoin("CONSIGN", "CONSIGN")
                .On(OOQL.CreateProperty("CONSIGN.CUSTOMER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID")
                    &OOQL.CreateProperty("CONSIGN.SALES_CENTER_ID") ==OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid")
                    &OOQL.CreateProperty("CONSIGN.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID"));

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "CONSIGN_TRANSFER_OUT", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        private void UpdateConsignTransferOut(IQueryService qrySrv, object[] ids) {
            if (ids == null || ids.Length <= 0)
                return;
            List<QueryProperty> idList = new List<QueryProperty>();
            ids.ToList().ForEach(c => idList.Add(OOQL.CreateConstants(c)));

            QueryNode node = 
                OOQL.Select(
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CURRENCY_ID"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.COMPANY_ID"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.EXCHANGE_RATE"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.TAX_ID"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.TAX_RATE"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.TAX_INCLUDED"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.AMOUNT"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.CONSIGN_TRANSFER_OUT_D_ID"),
                            OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.CONSIGN_TRANSFER_OUT_ID"))
                        .From("CONSIGN_TRANSFER_OUT", "CONSIGN_TRANSFER_OUT")
                        .InnerJoin("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_D", "CONSIGN_TRANSFER_OUT_D")
                        .On((OOQL.CreateProperty("CONSIGN_TRANSFER_OUT_D.CONSIGN_TRANSFER_OUT_ID") == OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_ID")))
                        .Where(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_ID").In(idList));
            DependencyObjectCollection consignTransferOutData = GetService<IQueryService>().ExecuteDependencyObject(node);
            if (consignTransferOutData.Count>0){
                //创建DataTable缓存金额数据用于批量更新至临时表
                DataTable dataTableConsignTransferOutD = CreateDataTableConsignTransferOutD();
                //给缓存DataTable写入值
                ITaxesService taxesSrv = GetService<ITaxesService>(TypeKey);
                foreach (var item in consignTransferOutData){
                    decimal[] taxes= taxesSrv.GetTaxes(
                        item["CURRENCY_ID"],
                        item["COMPANY_ID"],
                        item["EXCHANGE_RATE"].ToDecimal(),
                        item["TAX_ID"],
                        item["TAX_RATE"].ToDecimal(),
                        item["TAX_INCLUDED"].ToBoolean(),
                        item["AMOUNT"].ToDecimal()
                        );

                    DataRow dr = dataTableConsignTransferOutD.NewRow();
                    dr["CONSIGN_TRANSFER_OUT_D_ID"] = item["CONSIGN_TRANSFER_OUT_D_ID"];//主键
                    dr["CONSIGN_TRANSFER_OUT_ID"] = item["CONSIGN_TRANSFER_OUT_ID"];//父主键
                    dr["AMT_UNINCLUDE_TAX_OC"] = taxes[0];//原币未税金额
                    dr["TAX_OC"] = taxes[1];//原币税额
                    dr["AMT_UNINCLUDE_TAX_BC"] = taxes[2];//本币未税金额
                    dr["TAX_BC"] = taxes[3];//本币税额
                    dataTableConsignTransferOutD.Rows.Add(dr);
                }

                //创建map对照表
                //创建map对照表
                Dictionary<string, string> dicConsignTransferOut = dataTableConsignTransferOutD.Columns.Cast<DataColumn>().ToDictionary(key => key.ToStringExtension(), key => key.ToStringExtension());
                List<BulkCopyColumnMapping> consignTransferOutMap = UtilsClass.CreateBulkMapping(dicConsignTransferOut);
                //新增临时表
                IDataEntityType consignTransferOutDTmp = CreateTempTableConsignTransferOutD(qrySrv);

                //批量新增到临时表
                qrySrv.BulkCopy(dataTableConsignTransferOutD, consignTransferOutDTmp.Name, consignTransferOutMap.ToArray());
                
                //更新单身金额
                QueryNode update = OOQL.Update("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_D")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("AMT_UNINCLUDE_TAX_OC"),OOQL.CreateProperty("SubNode.AMT_UNINCLUDE_TAX_OC")),
                    new SetItem(OOQL.CreateProperty("TAX_OC"),OOQL.CreateProperty("SubNode.TAX_OC")),
                    new SetItem(OOQL.CreateProperty("AMT_UNINCLUDE_TAX_BC"),OOQL.CreateProperty("SubNode.AMT_UNINCLUDE_TAX_BC")),
                    new SetItem(OOQL.CreateProperty("TAX_BC"),OOQL.CreateProperty("SubNode.TAX_BC"))
                })
                .From(OOQL.Select(
                                OOQL.CreateProperty("CtodTmp.AMT_UNINCLUDE_TAX_OC"),
                                OOQL.CreateProperty("CtodTmp.TAX_OC"),
                                OOQL.CreateProperty("CtodTmp.AMT_UNINCLUDE_TAX_BC"),
                                OOQL.CreateProperty("CtodTmp.TAX_BC"),
                                OOQL.CreateProperty("CtodTmp.CONSIGN_TRANSFER_OUT_D_ID"))
                            .From(consignTransferOutDTmp.Name, "CtodTmp"), "SubNode")
                .Where(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_D.CONSIGN_TRANSFER_OUT_D_ID") == OOQL.CreateProperty("SubNode.CONSIGN_TRANSFER_OUT_D_ID"));
                UtilsClass.ExecuteNoQuery(qrySrv, update, false);

                //更新单头金额
                update = OOQL.Update("CONSIGN_TRANSFER_OUT")
                .Set(new SetItem[]{
                    new SetItem(OOQL.CreateProperty("AMT_UNINCLUDE_TAX_OC"),OOQL.CreateProperty("SubNode.AMT_UNINCLUDE_TAX_OC")),
                    new SetItem(OOQL.CreateProperty("TAX_OC"),OOQL.CreateProperty("SubNode.TAX_OC")),
                    new SetItem(OOQL.CreateProperty("AMT_UNINCLUDE_TAX_BC"),OOQL.CreateProperty("SubNode.AMT_UNINCLUDE_TAX_BC")),
                    new SetItem(OOQL.CreateProperty("TAX_BC"),OOQL.CreateProperty("SubNode.TAX_BC"))
                })
                .From(OOQL.Select(
                            OOQL.CreateProperty("CtodTmp.CONSIGN_TRANSFER_OUT_ID","CONSIGN_TRANSFER_OUT_ID"),
		                    Formulas.Sum(
                                    OOQL.CreateProperty("CtodTmp.AMT_UNINCLUDE_TAX_OC"), "AMT_UNINCLUDE_TAX_OC"),
		                    Formulas.Sum(
                                    OOQL.CreateProperty("CtodTmp.TAX_OC"), "TAX_OC"),
		                    Formulas.Sum(
                                    OOQL.CreateProperty("CtodTmp.AMT_UNINCLUDE_TAX_BC"), "AMT_UNINCLUDE_TAX_BC"),
		                    Formulas.Sum(
                                    OOQL.CreateProperty("CtodTmp.TAX_BC"), "TAX_BC"))
                        .From(consignTransferOutDTmp.Name, "CtodTmp")
                        .GroupBy(OOQL.CreateProperty("CtodTmp.CONSIGN_TRANSFER_OUT_ID")), "SubNode")
                .Where(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_ID") == OOQL.CreateProperty("SubNode.CONSIGN_TRANSFER_OUT_ID"));
                UtilsClass.ExecuteNoQuery(qrySrv, update, false);
            }
        }

        private DataTable CreateDataTableConsignTransferOutD() {
            string[] consignTransferOutColumns = new string[]{
                    "CONSIGN_TRANSFER_OUT_D_ID",  //主键
                    "CONSIGN_TRANSFER_OUT_ID",  //父主键
                    "AMT_UNINCLUDE_TAX_OC",  //原币未税金额
                    "TAX_OC",  //原币税额
                    "AMT_UNINCLUDE_TAX_BC",  //本币未税金额
                    "TAX_BC"//本币税额
            };
            DataTable consignTransferOutD = UtilsClass.CreateDataTable("CONSIGN_TRANSFER_OUT", consignTransferOutColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //父主键
                        typeof(decimal),  //原币未税金额
                        typeof(decimal),  //原币税额
                        typeof(decimal),  //本币未税金额
                        typeof(decimal)  //本币税额
                    });
            return consignTransferOutD;
        }

        private DependencyObjectType CreateTempTableConsignTransferOutD(IQueryService qrySrv) {
            string tempName = "TEMP_CONSIGN_TRANSFER_OUT_D" + "_" + DateTime.Now.ToString("HHmmssfff");
            DependencyObjectType defaultType = new DependencyObjectType(tempName, new Attribute[] { });
            IBusinessTypeService businessSrv = GetServiceForThisTypeKey<IBusinessTypeService>();

            #region 创建其他属性（字段）
            //CONSIGN_TRANSFER_OUT_ID	PrimaryKey	>>	主键
            defaultType.RegisterSimpleProperty("CONSIGN_TRANSFER_OUT_D_ID", businessSrv.SimplePrimaryKeyType, Maths.GuidDefaultValue(), false, new Attribute[] { businessSrv.SimplePrimaryKey });
            //CONSIGN_TRANSFER_OUT_ID	PrimaryKey	>>	父主键
            defaultType.RegisterSimpleProperty("CONSIGN_TRANSFER_OUT_ID", businessSrv.SimplePrimaryKeyType, Maths.GuidDefaultValue(), false, new Attribute[] { businessSrv.SimplePrimaryKey });
            //AMT_UNINCLUDE_TAX_OC	Amount	>>	原币未税金额
            defaultType.RegisterSimpleProperty("AMT_UNINCLUDE_TAX_OC", businessSrv.SimpleAmountType, 0m, false, new Attribute[] { businessSrv.SimpleAmount });
            //TAX_OC	Amount	>>	原币税额
            defaultType.RegisterSimpleProperty("TAX_OC", businessSrv.SimpleAmountType, 0m, false, new Attribute[] { businessSrv.SimpleAmount });
            //AMT_UNINCLUDE_TAX_BC	Amount	>>	本币未税金额
            defaultType.RegisterSimpleProperty("AMT_UNINCLUDE_TAX_BC", businessSrv.SimpleAmountType, 0m, false, new Attribute[] { businessSrv.SimpleAmount });
            //TAX_BC	Amount	>>	本币税额
            defaultType.RegisterSimpleProperty("TAX_BC", businessSrv.SimpleAmountType, 0m, false, new Attribute[] { businessSrv.SimpleAmount });
            #endregion
            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增寄售调拨单身
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpConsignTransferOutD"></param>
        private int InsertConsignTransferOutD(IQueryService qrySrv, IDataEntityType tmpConsignTransferOutD) {
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.AddRange(new QueryProperty[]{
                OOQL.CreateProperty("tmpTable.SequenceNumber","SequenceNumber"),  //序号
                OOQL.CreateProperty("tmpTable.CONSIGN_TRANSFER_OUT_D_ID","CONSIGN_TRANSFER_OUT_D_ID"),  //主键
                OOQL.CreateProperty("tmpTable.CONSIGN_TRANSFER_OUT_ID","CONSIGN_TRANSFER_OUT_ID"),  //父主键
                OOQL.CreateProperty("ITEM.ITEM_ID","ITEM_ID"),  //品号
                OOQL.CreateProperty("ITEM.ITEM_NAME","ITEM_DESCRIPTION"),  //品名
                OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_SPECIFICATION","ITEM_SPECIFICATION"),  //规格
                Formulas.IsNull(
                    Formulas.Case(null,
                        OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),
                        new CaseItem[]{
                            new CaseItem(OOQL.CreateProperty("tmpTable.item_feature_no")==OOQL.CreateConstants(string.Empty)
                                ,OOQL.CreateConstants(Maths.GuidDefaultValue()))}),
                        OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_FEATURE_ID"),  //特征码
                OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_TYPE","ITEM_TYPE"),  //商品类型
                OOQL.CreateProperty("tmpTable.picking_qty", "BUSINESS_QTY"),  //业务数量
                Formulas.Ext("UNIT_CONVERT", "SECOND_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.SECOND_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //第二数量
                Formulas.Ext("UNIT_CONVERT", "PRICE_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //计价数量
                Formulas.Ext("UNIT_CONVERT", "INVENTORY_QTY", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                        , OOQL.CreateConstants(0)}),  //库存数量
                OOQL.CreateProperty("SALES_ORDER_DOC_D.STANDARD_PRICE","STANDARD_PRICE"),  //标准单价
                OOQL.CreateProperty("SALES_ORDER_DOC_D.DISCOUNT_RATE","DISCOUNT_RATE"),  //折扣率
                OOQL.CreateProperty("SALES_ORDER_DOC_D.DISCOUNTED_PRICE","DISCOUNTED_PRICE"),  //折扣后单价
                OOQL.CreateArithmetic((OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE")
                        -OOQL.CreateProperty("SALES_ORDER_DOC_D.DISCOUNTED_PRICE"))
				        ,Formulas.Ext("UNIT_CONVERT", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE_UNIT_ID")
                        , OOQL.CreateConstants(0)})
                        ,ArithmeticOperators.Mulit,"DISCOUNT_AMT"),//折扣额
                OOQL.CreateProperty("SALES_ORDER_DOC_D.STANDARD_DISCOUNT_RATE","STANDARD_DISCOUNT_RATE"),  //标准折扣率
                OOQL.CreateArithmetic(
                        (Formulas.Ext("UNIT_CONVERT", new object[]{ OOQL.CreateProperty("ITEM.ITEM_ID")
                        , OOQL.CreateProperty("UNIT.UNIT_ID")
                        , OOQL.CreateProperty("tmpTable.picking_qty")
                        , OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE_UNIT_ID")
                        , OOQL.CreateConstants(0)}))
				        ,OOQL.CreateProperty("SALES_ORDER_DOC_D.DISCOUNTED_PRICE"),ArithmeticOperators.Mulit,"AMOUNT"),//金额
                Formulas.Cast(OOQL.CreateConstants(0m),GeneralDBType.Decimal,"AMT_UNINCLUDE_TAX_OC"),//原币未税金额
                Formulas.Cast(OOQL.CreateConstants(0m),GeneralDBType.Decimal,"TAX_OC"),//原币税额
                Formulas.Cast(OOQL.CreateConstants(0m),GeneralDBType.Decimal,"AMT_UNINCLUDE_TAX_BC"),//本币未税金额
                Formulas.Cast(OOQL.CreateConstants(0m),GeneralDBType.Decimal,"TAX_BC"),//本币税额
                OOQL.CreateConstants(string.Empty,"REMARK"),//备注
                OOQL.CreateProperty("SALES_ORDER_DOC_D.TAX_ID","TAX_ID"),  //税种
                OOQL.CreateProperty("SALES_ORDER_DOC_D.TAX_RATE","TAX_RATE"),  //税率
                OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE","PRICE"),  //单价
                OOQL.CreateConstants(string.Empty,"PACKING_QTY"),//包装数量描述
                OOQL.CreateConstants(0m,"PACKING_QTY1"),//大包装数量
                OOQL.CreateConstants(0m,"PACKING_QTY2"),//中包装数量
                OOQL.CreateConstants(0m,"PACKING_QTY3"),//小包装数量
                OOQL.CreateConstants(0m,"PACKING_QTY4"),//最小包装数量
                OOQL.CreateConstants(0,"PIECES"),//件数
                OOQL.CreateConstants("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD","SOURCE_ID.RTK"),//源单RTK
                OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_SD_ID","SOURCE_ID.ROid"),  //源单ROid
                OOQL.CreateProperty("SALES_ORDER_DOC_D.BUSINESS_UNIT_ID","BUSINESS_UNIT_ID"),	//业务单位
                OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE_UNIT_ID","PRICE_UNIT_ID"),	//计价单位
                OOQL.CreateProperty("SALES_ORDER_DOC_D.PRICE_TABLE_ID","PRICE_TABLE_ID"),	//价格表
                OOQL.CreateProperty("SALES_ORDER_DOC_D.DISCOUNT_TABLE_ID","DISCOUNT_TABLE_ID"),	//折扣表
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"PACKING_MODE_ID"),//包装方式
                Formulas.IsNull(
				    OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()),"WAREHOUSE_ID"),	//仓库
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"PACKING1_UNIT_ID"),	//大包装单位
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"PACKING2_UNIT_ID"),	//中包装单位
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"PACKING3_UNIT_ID"),	//小包装单位
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"PACKING4_UNIT_ID"),	//最小包装单位
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"),
                    new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("tmpTable.lot_no")==OOQL.CreateConstants(string.Empty)
                            ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                    }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_LOT_ID"),  //批号
                Formulas.IsNull(Formulas.Case(null,OOQL.CreateProperty("BIN.BIN_ID"),
                    new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no")==OOQL.CreateConstants(string.Empty)
                            ,OOQL.CreateConstants(Maths.GuidDefaultValue()))
                    }),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BIN_ID"),  //库位
                OOQL.CreateProperty("SALES_ORDER_DOC_D.CUSTOMER_ITEM_ID","CUSTOMER_ITEM_ID"),	//客户品号
                Formulas.Case(null,OOQL.CreateConstants("0"),
                    new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("CUSTOMER_SALES.SIGN_REQUIRED")==OOQL.CreateConstants(true)
                            ,OOQL.CreateConstants("1"))},"CUSTOMER_SIGNED"),  //签收状态
                OOQL.CreateConstants(0,"CUSTOMER_SIGNED_QTY"),	//业务签收数量
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"CONSIGN_TRANSFER_IN_ID"),	//退回单号
                OOQL.CreateConstants(Maths.GuidDefaultValue(),"RETURN_REASON_ID"),	//签收差异原因
                Formulas.Case(null,
				    OOQL.CreateConstants("COST_DOMAIN"),
				    OOQL.CreateCaseArray(
						    OOQL.CreateCaseItem((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants("1")),
								    OOQL.CreateConstants("COMPANY"))),"FROM_COST_DOMAIN_ID.RTK"),
                Formulas.Case(
                    OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL"),
                    OOQL.CreateCaseArray(
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("1"),
				                    OOQL.CreateProperty("PLANT.COMPANY_ID")),
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("2"),
				                    OOQL.CreateProperty("PLANT.COST_DOMAIN_ID")),
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("3"),
				                    OOQL.CreateProperty("WAREHOUSE.COST_DOMAIN_ID"))),"FROM_COST_DOMAIN_ID.ROid"),
                Formulas.Case(null,
				    OOQL.CreateConstants("COST_DOMAIN"),
				    OOQL.CreateCaseArray(
						    OOQL.CreateCaseItem((OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL") == OOQL.CreateConstants("1")),
								    OOQL.CreateConstants("COMPANY"))),"TO_COST_DOMAIN_ID.RTK"),
                Formulas.Case(
                    OOQL.CreateProperty("PARA_COMPANY.INVENTORY_VALUATION_LEVEL"),
                    OOQL.CreateCaseArray(
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("1"),
				                    OOQL.CreateProperty("PLANT.COMPANY_ID")),
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("2"),
				                    OOQL.CreateProperty("PLANT1.COST_DOMAIN_ID")),
		                    OOQL.CreateCaseItem(
				                    OOQL.CreateConstants("3"),
				                    OOQL.CreateProperty("WAREHOUSE1.COST_DOMAIN_ID"))),"TO_COST_DOMAIN_ID.ROid"),
                Formulas.Case(null,OOQL.CreateConstants("1"),
                    new CaseItem[]{
                        new CaseItem(OOQL.CreateProperty("DOC.BC_CHECK")==OOQL.CreateConstants("0")
                            ,OOQL.CreateConstants("0"))},"BC_CHECK_STATUS"),  //检核码
                OOQL.CreateProperty("SALES_ORDER_DOC_SD.PROJECT_ID","PROJECT_ID"),	//项目
                OOQL.CreateConstants(0,"UNIT_COST"),	//单位成本
                OOQL.CreateConstants(0,"COST_AMT"),	//成本金额
                OOQL.CreateConstants(0,"SN_COLLECTED_QTY"),	//序列号已采集数量
                Formulas.Case(null,OOQL.CreateConstants("1"),
                    new CaseItem[]{
                        new CaseItem((OOQL.CreateProperty("ITEM.ITEM_SN_MANAGEMENT")==OOQL.CreateConstants(false))
                            | (OOQL.CreateProperty("tmpTable.picking_qty")==OOQL.CreateConstants(0))
                            ,OOQL.CreateConstants("0"))},"SN_COLLECTED_STATUS")  //序列号检核码
            });
            #endregion

            QueryNode groupNode = GroupNode(tmpConsignTransferOutD, true);
            QueryNode insertNode = OOQL.Select(
                     properties
                 )
                .From(groupNode, "tmpTable")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("tmpTable.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("PARA_COMPANY")
                .On(OOQL.CreateProperty("PARA_COMPANY.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.COMPANY_ID"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("tmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID")
                    & OOQL.CreateProperty("tmpTable.item_feature_no") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("WAREHOUSE")
                .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                    & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"))
                .LeftJoin("SALES_ORDER_DOC")
                .On(OOQL.CreateProperty("tmpTable.doc_no") == OOQL.CreateProperty("SALES_ORDER_DOC.DOC_NO"))
                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D", "SALES_ORDER_DOC_D")
                .On(OOQL.CreateProperty("tmpTable.seq") == OOQL.CreateProperty("SALES_ORDER_DOC_D.SequenceNumber")
                    & OOQL.CreateProperty("SALES_ORDER_DOC.SALES_ORDER_DOC_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_ID"))
                .LeftJoin("SALES_ORDER_DOC.SALES_ORDER_DOC_D.SALES_ORDER_DOC_SD", "SALES_ORDER_DOC_SD")
                .On(OOQL.CreateProperty("SALES_ORDER_DOC_D.SALES_ORDER_DOC_D_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_SD.SALES_ORDER_DOC_D_ID"))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("tmpTable.lot_no") == OOQL.CreateProperty("ITEM_LOT.LOT_CODE")
                    & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_ID")
                    & ((OOQL.CreateProperty("tmpTable.item_feature_no") == OOQL.CreateConstants("")
                        & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))
                        | (OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID"))))
                .InnerJoin("CUSTOMER_SALES")
                .On(OOQL.CreateProperty("CUSTOMER_SALES.CUSTOMER_ID") == OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID")
                    & OOQL.CreateProperty("CUSTOMER_SALES.Owner_Org.ROid") == OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid"))
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("tmpTable.doc_id") == OOQL.CreateProperty("DOC.DOC_ID"))
                .LeftJoin("CONSIGN", "CONSIGN")
                .On((OOQL.CreateProperty("CONSIGN.CUSTOMER_ID") == OOQL.CreateProperty("SALES_ORDER_DOC.SHIP_TO_CUSTOMER_ID"))
                    & (OOQL.CreateProperty("CONSIGN.SALES_CENTER_ID") == OOQL.CreateProperty("SALES_ORDER_DOC.Owner_Org.ROid"))
                    & (OOQL.CreateProperty("CONSIGN.PLANT_ID") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                .LeftJoin("ITEM_PLANT", "ITEM_PLANT")
                .On((OOQL.CreateProperty("ITEM_PLANT.ITEM_ID") == OOQL.CreateProperty("SALES_ORDER_DOC_D.ITEM_ID"))
                    & (OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID")))
                .LeftJoin("WAREHOUSE", "WAREHOUSE1")
                .On(OOQL.CreateProperty("WAREHOUSE1.WAREHOUSE_ID") == OOQL.CreateProperty("CONSIGN.WAREHOUSE_ID"))
                .LeftJoin("PLANT", "PLANT1")
                .On(OOQL.CreateProperty("PLANT1.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE1.Owner_Org.ROid"));

            //执行插入
           return UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_D", insertNode, properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 利用临时表关联实体表进行批量新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpConsignTransferOutD"></param>
        private void InsertBcLine(IQueryService qrySrv, IDataEntityType tmpConsignTransferOutD) {
            bool bcLintFlag = UtilsClass.IsBCLineManagement(qrySrv);
            if (!bcLintFlag)return;
            //按照(单身调出仓库)
            SeparatingWarehouseInsertBcLine(qrySrv, tmpConsignTransferOutD, false);
            //按照(单头寄售仓)
            SeparatingWarehouseInsertBcLine(qrySrv, tmpConsignTransferOutD,true);
        }

        /// <summary>
        /// 区分仓库新增条码交易明细
        /// </summary>
        /// <param name="qrySrv"></param>
        /// <param name="tmpConsignTransferOutD"></param>
        /// <param name="isConsignWarehouseId">生成的仓库如果取CONSIGN_TRANSFER_OUT.CONSIGN_WAREHOUSE_ID传入TRUE，如果取WAREHOUSE.WAREHOUSE_ID则传入FALSE</param>
        private void SeparatingWarehouseInsertBcLine(IQueryService qrySrv, IDataEntityType tmpConsignTransferOutD,bool isConsignWarehouseId){
            #region properties
            List<QueryProperty> properties = new List<QueryProperty>();
            properties.Add(Formulas.NewId("BC_LINE_ID")); //主键
                properties.Add(OOQL.CreateProperty("tmpTable.barcode_no", "BARCODE_NO")); //条码CODE
                properties.Add(OOQL.CreateConstants("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_D", "SOURCE_ID.RTK")); //来源单据类型
                properties.Add(OOQL.CreateProperty("tmpTable.CONSIGN_TRANSFER_OUT_D_ID", "SOURCE_ID.ROid")); //来源单据
                properties.Add(Formulas.Ext("UNIT_CONVERT", "QTY", new object[]{
                    OOQL.CreateProperty("ITEM.ITEM_ID")
                    , OOQL.CreateProperty("UNIT.UNIT_ID")
                    , OOQL.CreateProperty("tmpTable.picking_qty")
                    , OOQL.CreateProperty("ITEM.STOCK_UNIT_ID")
                    , OOQL.CreateConstants(0)
                })); //数量

            if (isConsignWarehouseId){
                properties.Add(Formulas.IsNull(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_WAREHOUSE_ID"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID")); //仓库
                properties.Add(OOQL.CreateConstants(Maths.GuidDefaultValue(), "BIN_ID")); //库位
            }
            else{
                properties.Add(Formulas.IsNull(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()), "WAREHOUSE_ID")); //仓库
                properties.Add(Formulas.IsNull(Formulas.Case(null, OOQL.CreateProperty("BIN.BIN_ID"), new CaseItem[]{
                    new CaseItem(OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateConstants(string.Empty)
                        , OOQL.CreateConstants(Maths.GuidDefaultValue()))
                }), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID")); //库位
            }

            properties.Add(Formulas.IsNull(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.Owner_Org.RTK"), OOQL.CreateConstants(string.Empty),
                    "Owner_Org.RTK"));
                properties.Add(Formulas.IsNull(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.Owner_Org.ROid"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()), "Owner_Org.ROid"));
                properties.Add(Formulas.IsNull(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_ID"),
                    OOQL.CreateConstants(Maths.GuidDefaultValue()), "SOURCE_DOC_ID"));
                properties.Add(Formulas.IsNull(OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.DOC_DATE"),
                    OOQL.CreateConstants(OrmDataOption.EmptyDateTime), "DOC_DATE"));
            #endregion

            QueryNode groupNode = GroupNode(tmpConsignTransferOutD, false);
            JoinOnNode insertNode = OOQL.Select(properties)
                .From(groupNode, "tmpTable")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("tmpTable.site_no") == OOQL.CreateProperty("PLANT.PLANT_CODE"))
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("tmpTable.item_no") == OOQL.CreateProperty("ITEM.ITEM_CODE"))
                .InnerJoin("UNIT")
                .On(OOQL.CreateProperty("tmpTable.picking_unit_no") == OOQL.CreateProperty("UNIT.UNIT_CODE"))
                .LeftJoin("CONSIGN_TRANSFER_OUT", "CONSIGN_TRANSFER_OUT")
                .On(OOQL.CreateProperty("tmpTable.CONSIGN_TRANSFER_OUT_ID") ==OOQL.CreateProperty("CONSIGN_TRANSFER_OUT.CONSIGN_TRANSFER_OUT_ID"));
            if (!isConsignWarehouseId){
                insertNode=insertNode
                    .LeftJoin("WAREHOUSE")
                    .On(OOQL.CreateProperty("tmpTable.warehouse_no") == OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE")
                        & OOQL.CreateProperty("PLANT.PLANT_ID") == OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid"))
                    .LeftJoin("WAREHOUSE.BIN", "BIN")
                    .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                        & OOQL.CreateProperty("tmpTable.storage_spaces_no") == OOQL.CreateProperty("BIN.BIN_CODE"));
            }

            //执行插入
            UtilsClass.InsertDocumentToDataBaseOrTmpTable(qrySrv, "BC_LINE", insertNode,
                properties.Select(c => c.Alias).ToArray());
        }

        /// <summary>
        /// 对单身分组
        /// 单身和条码明显的分组依据不一样
        /// </summary>
        /// <param name="tmpIssueReceiptD"></param>
        /// <param name="isEntityLine"></param>
        /// <returns></returns>
        public QueryNode GroupNode(IDataEntityType tmpIssueReceiptD, bool isEntityLine) {
            List<QueryProperty> properties = new List<QueryProperty>{OOQL.CreateProperty("TMP.CONSIGN_TRANSFER_OUT_D_ID")
                    , OOQL.CreateProperty("TMP.CONSIGN_TRANSFER_OUT_ID")
                    , OOQL.CreateProperty("TMP.info_lot_no")
                    , OOQL.CreateProperty("TMP.SequenceNumber")
                    , OOQL.CreateProperty("TMP.item_no")
                    , OOQL.CreateProperty("TMP.item_feature_no")
                    , OOQL.CreateProperty("TMP.picking_unit_no")
                    , OOQL.CreateProperty("TMP.doc_no")
                    , OOQL.CreateProperty("TMP.seq")
                    , OOQL.CreateProperty("TMP.warehouse_no")
                    , OOQL.CreateProperty("TMP.storage_spaces_no")
                    , OOQL.CreateProperty("TMP.lot_no")
                    , OOQL.CreateProperty("TMP.site_no")
                    , OOQL.CreateProperty("TMP.doc_id")
            };

            List<QueryProperty> groupProperties = new List<QueryProperty>();
            groupProperties.AddRange(properties);

            if (!isEntityLine) {
                properties.Add(OOQL.CreateProperty("TMP.barcode_no"));
                groupProperties.Add(OOQL.CreateProperty("TMP.barcode_no"));
            }

            properties.Add(Formulas.IsNull(Formulas.Sum(OOQL.CreateProperty("TMP.picking_qty")), OOQL.CreateConstants(0), "picking_qty"));

            QueryNode node = OOQL.Select(properties
                )
                .From(tmpIssueReceiptD.Name, "TMP")
                .GroupBy(groupProperties);

            return node;
        }

        /// <summary>
        /// 释放内存
        /// </summary>
        private void ClearData() {
            _currentDocNo = null;
            _docInfos = null;
            _documentNumberGenSrv = null;
            _encodeSrv = null;
            _primaryKeySrv = null;
        }

        /// <summary>
        /// 创建批量修改所需要的DataTable和Mapping
        /// </summary>
        private void CreateRelateTable(ref DataTable consignTransferOut, ref DataTable consignTransferOutD
            , ref List<BulkCopyColumnMapping> consignTransferOutMap, ref List<BulkCopyColumnMapping> consignTransferOutDMap) {
            #region 创建寄售调拨单表
            string[] consignTransferOutColumns = new string[]{
                    "ID",  //主键
                    "doc_id",  //单据类型
                    "doc_no",  //单号
                    "company_id",  //工厂对应的公司
                    "employee_no",  //人员
                    "picking_department_no",  //部门
                    "site_no",  //工厂编号
                    "info_lot_no",        //信息批号
            };
            consignTransferOut = UtilsClass.CreateDataTable("CONSIGN_TRANSFER_OUT", consignTransferOutColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //单据类型
                        typeof(string),  //单号
                        typeof(object),  //工厂对应公司ID
                        typeof(string),  //人员
                        typeof(string),  //部门
                        typeof(string),  //工厂编号
                        typeof(string), //信息批号
                    });

            //创建map对照表
            Dictionary<string, string> dicConsignTransferOut = new Dictionary<string, string>();
            foreach (string key in consignTransferOutColumns){
                dicConsignTransferOut.Add(key, key);
            }
            consignTransferOutMap = UtilsClass.CreateBulkMapping(dicConsignTransferOut);
            #endregion

            #region 创建寄售调拨单身表
            string[] consignTransferOutDColumns = new string[]{
                    "CONSIGN_TRANSFER_OUT_D_ID",  //主键
                    "CONSIGN_TRANSFER_OUT_ID",  //父主键
                    "info_lot_no",  //信息批号
                    "SequenceNumber",  //序号
                    "item_no",  //品号
                    "item_feature_no",   //特征码
                    "picking_unit_no",  //单位
                    "doc_no",  //单号
                    "seq",  //来源序号
                    "warehouse_no",  //仓库
                    "storage_spaces_no",   //库位
                    "lot_no",  //批号
                    "picking_qty",  //拣货数量
                    "barcode_no",  //条码编号
                    "site_no",  //工厂编码
                    "doc_id"  //单据类型
            };
            consignTransferOutD = UtilsClass.CreateDataTable("CONSIGN_TRANSFER_OUT_D", consignTransferOutDColumns,
                    new Type[]{
                        typeof(object),  //主键
                        typeof(object),  //父主键
                        typeof(string),  //信息批号
                        typeof(int),  //序号
                        typeof(string),  //品号
                        typeof(string),   //特征码
                        typeof(string),  //单位
                        typeof(string),   //单号
                        typeof(string),  //来源序号
                        typeof(string),   //仓库
                        typeof(string),  //库位
                        typeof(string),   //批号
                        typeof(decimal),  //拣货数量
                        typeof(string),   //条码编号
                        typeof(string),   //工厂编码
                        typeof(object)  //单据类型
                    });

            //创建map对照表
            Dictionary<string, string> dicConsignTransferOutD = new Dictionary<string, string>();
            foreach (string key in consignTransferOutDColumns)
                dicConsignTransferOutD.Add(key, key);
            consignTransferOutDMap = UtilsClass.CreateBulkMapping(dicConsignTransferOutD);
            #endregion
        }

        /// <summary>
        /// 存储所需新增ConsignTransferOut的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateConsignTransferOutTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertConsignTransferOut_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv = GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;

            #region 字段
            //销货出口单主键
            defaultType.RegisterSimpleProperty("ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单据类型
            defaultType.RegisterSimpleProperty("doc_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });
            //工厂的公司
            defaultType.RegisterSimpleProperty("company_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //人员
            defaultType.RegisterSimpleProperty("employee_no", businessSrv.SimpleBusinessCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleBusinessCode });
            //部门
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("picking_department_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //工厂编号
            defaultType.RegisterSimpleProperty("site_no", businessSrv.SimpleFactoryType, string.Empty, false, new Attribute[] { businessSrv.SimpleFactory });
            //信息批号
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        /// <summary>
        /// 存储所需新增ConsignTransferOutD的数据集合的临时表
        /// </summary>
        private IDataEntityType CreateConsignTransferOutDTmpTable(IQueryService qrySrv) {
            string typeName = "Temp_InsertConsignTransferOutD_" + DateTime.Now.ToString("HHmmssfff");// 临时表表名的处理
            DependencyObjectType defaultType = new DependencyObjectType(typeName, new Attribute[] { });

            IBusinessTypeService businessSrv =GetServiceForThisTypeKey<IBusinessTypeService>();
            SimplePropertyAttribute simplePrimaryAttri = businessSrv.SimplePrimaryKey;

            #region 字段
            //主键
            defaultType.RegisterSimpleProperty("CONSIGN_TRANSFER_OUT_D_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //父主键
            defaultType.RegisterSimpleProperty("CONSIGN_TRANSFER_OUT_ID", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            //信息批号
            SimplePropertyAttribute tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("info_lot_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("SequenceNumber", typeof(int), 0, false, new Attribute[] { tempAttr });
            //品号
            defaultType.RegisterSimpleProperty("item_no", businessSrv.SimpleItemCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemCode });
            //特征码
            defaultType.RegisterSimpleProperty("item_feature_no", businessSrv.SimpleItemFeatureType, string.Empty, false, new Attribute[] { businessSrv.SimpleItemFeature });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            //单位
            defaultType.RegisterSimpleProperty("picking_unit_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //单号
            defaultType.RegisterSimpleProperty("doc_no", businessSrv.SimpleDocNoType, string.Empty, false, new Attribute[] { businessSrv.SimpleDocNo });
            //来源序号
            tempAttr = new SimplePropertyAttribute(GeneralDBType.Int32);
            defaultType.RegisterSimpleProperty("seq", typeof(int), 0, false, new Attribute[] { tempAttr });
            //仓库
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            defaultType.RegisterSimpleProperty("warehouse_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //库位
            defaultType.RegisterSimpleProperty("storage_spaces_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //批号
            defaultType.RegisterSimpleProperty("lot_no", businessSrv.SimpleLotCodeType, string.Empty, false, new Attribute[] { businessSrv.SimpleLotCode });
            //拣货数量
            defaultType.RegisterSimpleProperty("picking_qty", businessSrv.SimpleQuantityType, 0m, false, new Attribute[] { businessSrv.SimpleQuantity });
            //条码编号
            defaultType.RegisterSimpleProperty("barcode_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            tempAttr = new SimplePropertyAttribute(GeneralDBType.String);
            //工厂编码
            defaultType.RegisterSimpleProperty("site_no", typeof(string), string.Empty, false, new Attribute[] { tempAttr });
            //单据类型
            defaultType.RegisterSimpleProperty("doc_id", businessSrv.SimplePrimaryKeyType, null, false, new Attribute[] { simplePrimaryAttri });
            #endregion

            qrySrv.CreateTempTable(defaultType);

            return defaultType;
        }

        /// <summary>
        /// 单据生成结果结构
        /// </summary>
        public class DocumentInfo {
            /// <summary>
            /// ID
            /// </summary>
            public object Id { get; set; }
            /// <summary>
            /// 单据类型
            /// </summary>
            public object DocId { get; set; }
            /// <summary>
            /// 组织
            /// </summary>
            public object OwnerOrgId { get; set; }
            /// <summary>
            /// 单号
            /// </summary>
            public string DocNo { get; set; }
        }

        /// <summary>
        /// 分组之后的实体行信息
        /// </summary>
        public class EntityLine {
            /// <summary>
            /// 决定唯一性的相关字段
            /// </summary>
            public string UniqueKey { get; set; }

            /// <summary>
            /// 行ID
            /// </summary>
            public object Key { get; set; }

            /// <summary>
            /// 行序号
            /// </summary>
            public int SequenceNumber { get; set; }
        }

        #endregion
    }
}
