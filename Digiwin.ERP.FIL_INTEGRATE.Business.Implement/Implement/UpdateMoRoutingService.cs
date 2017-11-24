//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/16 10:09:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>更新工单工艺信息服务</description>
//20161208 modi by shenbao fro P001-161208001
//20161213 modi by shenbao for B001-161213006 校验单据类型
//20170223 modi by shenbao for P001-170221002 修正单据类型取值
//20170406 modi by wangyq for P001-170327001 +补上lot_no，其他比规格缺少的字段跟sd确认后是无用的,无需添加
//20170612 modi by shenbao for P001-170606002 同步3.0
//20170619 modi by zhangcn for P001-170606002
//20170727 modi by shenbao for P001-170717001 若APP提交人时或者机时为0，且系统启用上下线管理，则报工单时数根据上下线时间自动推算
//20170801 modi by shenbao for P001-170717001

using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Text;
using Digiwin.Common;
using System.ComponentModel;
using Digiwin.Common.Torridity;
using Digiwin.Common.Query2;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using System.Collections.ObjectModel;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.Common.Services;
using Digiwin.ERP.WIP_TRANSFER_DOC.Business;
using Digiwin.Common.Core;
using System.Collections;
using System.Globalization;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {
    [SingleGetCreator]
    [ServiceClass(typeof(IUpdateMoRoutingService))]
    [Description("更新工单工艺信息服务")]
    public sealed class UpdateMoRoutingService : ServiceComponent, IUpdateMoRoutingService {
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

        private ISysParameterService _sysParaSrv;
        /// <summary>
        /// 系统参数服务
        /// </summary>
        public ISysParameterService SysParaSrv {
            get {
                if (_sysParaSrv == null)
                    _sysParaSrv = this.GetService<ISysParameterService>();

                return _sysParaSrv;
            }
        }

        private IDocumentNumberGenerateService _documentNumberGenSrv;
        /// <summary>
        /// 生成单号服务
        /// </summary>
        public IDocumentNumberGenerateService DocumentNumberGenSrv {
            get {
                if (_documentNumberGenSrv == null)
                    _documentNumberGenSrv = this.GetService<IDocumentNumberGenerateService>("WIP_TRANSFER_DOC");

                return _documentNumberGenSrv;
            }
        }

        #endregion

        #region 自定义字段

        /// <summary>
        /// 接口参数对象
        /// </summary>
        private ParaObject _paraObject = null;

        /// <summary>
        /// 工作中心信息
        /// </summary>
        private DependencyObject _queryWorkCenter = null;

        /// <summary>
        /// 工单工艺信息
        /// </summary>
        private DependencyObject _queryMoRouting = null;

        /// <summary>
        /// 单据类型
        /// </summary>
        private DependencyObject _queryDoc = null;

        /// <summary>
        /// 改对象集合了规格中的QueryMoProduct、QueryItemLot、QueryMachine、QueryMachineTeam、QueryWorkTeam、QueryEmployee
        /// 所查出来的数据
        /// </summary>
        private DependencyObject _queryDataInfo = null;

        private DependencyObject _queryMoRoutingPath = null;

        /// <summary>
        /// 启用上线
        /// </summary>
        private bool _paraCheckOut = false;  //20170727 add by shenbao for P001-170717001

        /// <summary>
        /// 启用下线
        /// </summary>
        private bool _paraCheckIn = false;  //20170801 add by shenbao for P001-170717001

        #endregion

        #region IUpdateMoRoutingService 成员

        /// <summary>
        /// 更新工单工艺信息
        /// </summary>
        /// <param name="report_type">报工类别</param>
        /// <param name="wo_no">工单号码</param>
        /// <param name="run_card_no">Run Card</param>
        /// <param name="op_no">作业编号</param>
        /// <param name="op_seq">作业序</param>
        /// <param name="workstation_no">工作站</param>
        /// <param name="machine_no">机器编号</param>
        /// <param name="shift_no">报工班别</param>
        /// <param name="labor_hours">工时</param>
        /// <param name="machine_hours">机时</param>
        /// <param name="reports_qty">报工数量</param>
        /// <param name="scrap_qty">报废数量</param>
        /// <param name="item_no">生产料号</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        public Hashtable UpdateMoRouting(string site_no, string report_type, string wo_no, string run_card_no
            , string op_no, string op_seq, string workstation_no, string machine_no
            , int labor_hours, int machine_hours, decimal reports_qty
            , decimal scrap_qty, string item_no, string shift_no, string employee_no, string employee_name
            , string lot_no, string warehouse_no, string storage_spaces_no//20170406 add by wangyq for P001-170327001 先补上lot_no，其他比规格缺少的字段跟sd确认后是无用的,无需添加
            ) {
            Hashtable rtnHash = new Hashtable();
            DependencyObjectCollection rtnColl = CreateReturnCollection();
            //组建参数类
            _paraObject = new ParaObject(employee_no, report_type, wo_no, run_card_no, op_no, op_seq, workstation_no, machine_no
                , shift_no, labor_hours, machine_hours, reports_qty.ToDecimal(), scrap_qty.ToDecimal(), item_no, site_no
                , lot_no, warehouse_no, storage_spaces_no//20170406 add by wangyq for P001-170327001
                );
            #region 参数检查
            if (Maths.IsEmpty(report_type)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "report_type" }));
            }
            if (Maths.IsEmpty(wo_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "wo_no" }));
            }
            if (Maths.IsEmpty(op_no)) {
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111201", new object[] { "op_no" }));
            }
            #endregion

            //查询工作中心和工单工艺信息
            QueryWorkCenter();
            if (Maths.IsEmpty(_queryWorkCenter))
                throw new BusinessRuleException("QueryWorkCenter");
            QueryMoRouting();
            if (Maths.IsEmpty(_queryMoRouting))
                throw new BusinessRuleException("QueryMoRouting");
            
            using (ITransactionService trans = this.GetService<ITransactionService>()) {
                if (report_type == "1") {
                    //7.1 投产
                    MoveIn(rtnColl);
                } else if (report_type == "2") {
                    //7.2 上线
                    QueryCheckOutControl();  //20170801 add by shenbao for P001-170717001
                    CheckIn(rtnColl);
                } else if (report_type == "4") {
                    //7.3 下线
                    CheckOut(rtnColl);
                } else if (report_type == "3" || report_type == "5"
                    || report_type == "6"//20170406 add by wangyq for P001-170327001
                    ) {
                    //7.4 转移
                    QueryCheckOutControl();  //20170801 add by shenbao for P001-170717001
                    MoveOut(rtnColl, report_type);
                }

                trans.Complete();
            }

            //组织返回值
            if (rtnColl.Count > 0) {
                foreach (DependencyProperty property in rtnColl.ItemDependencyObjectType.Properties)
                    rtnHash.Add(property.Name, rtnColl[0][property.Name]);
            }

            //释放内存
            ClearData();

            return rtnHash;
        }

        #endregion

        #region 自定义方法

        /// <summary>
        /// 创建服务返回集合
        /// </summary>
        /// <returns></returns>
        private DependencyObjectCollection CreateReturnCollection() {
            DependencyObjectType type = new DependencyObjectType("ReturnCollection");
            type.RegisterSimpleProperty("code", typeof(int));
            type.RegisterSimpleProperty("sql_code", typeof(string));
            type.RegisterSimpleProperty("description", typeof(string));
            type.RegisterSimpleProperty("report_no", typeof(string));

            DependencyObjectCollection Rtn = new DependencyObjectCollection(type);

            return Rtn;
        }

        private void SetValue(DependencyObjectCollection coll, int code, string sqlCode, string description, string reportNo) {
            DependencyObject obj = coll.AddNew();
            obj["code"] = code;
            obj["sql_code"] = sqlCode;
            obj["description"] = description;
            obj["report_no"] = reportNo;
        }

        private void QueryWorkCenter() {
            QueryNode node = OOQL.Select("WORK_CENTER.WORK_CENTER_ID"
                    , "WORK_CENTER.DISPATCH_CONTROL", "WORK_CENTER.Owner_Dept")
                .From("WORK_CENTER", "WORK_CENTER")
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("WORK_CENTER.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .Where((OOQL.AuthFilter("WORK_CENTER", "WORK_CENTER"))
                    & (OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_CODE") == OOQL.CreateConstants(_paraObject.workstation_no)
                    & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(_paraObject.site_no)));

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            if (result.Count > 0)
                _queryWorkCenter = result[0];
        }

        private void QueryMoRouting() {
            QueryNode node = OOQL.Select("PLANT.PLANT_ID"
                    , "MO_ROUTING_D.MO_ROUTING_D_ID"
                    , "MO_ROUTING_D.PRE_PROCESSING_UNIT_ID"  //加工前单位
                    , "MO_ROUTING_D.AFTER_PROCESSING_UNIT_ID"  //加工后单位
                    , "MO_ROUTING_D.INSPECT_MODE"  //质检模式
                    , "MO_ROUTING_D.OPERATION_ID"  //工艺
                    , "MO_ROUTING_D.POSITION_FLAG"  //起讫标记
                    , "MO_ROUTING_D.PIECE_PRICE"  //计件单价
                    , "MO.MO_ID"  //工单
                    , "MO_ROUTING_WIP.FIRST_CHECKIN_TIME"  //最早上线时间
                    , "MO_ROUTING_WIP.LAST_CHECKOUT_DATETIME"  //最晚下线时间
                    , "MO_ROUTING_D.LABOR_RATIO" //工时工资率
                    , "MO.DOC_ID"  //20170223 add by shenbao for P001-170221002
                    , "MO_ROUTING_D.STANDARD_MACHINE_HOUR"  //标准机时(秒)  //20170727 add by shenbao for P001-170717001
                    , "MO_ROUTING_D.STANDARD_MAN_HOUR"  //标准人时(秒)  //20170727 add by shenbao for P001-170717001
                )
                .From("MO_ROUTING", "MO_ROUTING")
                .InnerJoin("MO")
                .On(OOQL.CreateProperty("MO_ROUTING.MO_ID") == OOQL.CreateProperty("MO.MO_ID"))
                .InnerJoin("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                .On(OOQL.CreateProperty("MO_ROUTING.MO_ROUTING_ID") == OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_ID"))
                .InnerJoin("OPERATION")
                .On(OOQL.CreateProperty("MO_ROUTING_D.OPERATION_ID") == OOQL.CreateProperty("OPERATION.OPERATION_ID"))
                .InnerJoin("PLANT")
                .On(OOQL.CreateProperty("MO.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                .LeftJoin("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_WIP", "MO_ROUTING_WIP")
                .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("MO_ROUTING_WIP.MO_ROUTING_D_ID")
                    & OOQL.CreateProperty("MO_ROUTING_WIP.SOURCE_ID.ROid") == OOQL.CreateConstants(_queryWorkCenter["WORK_CENTER_ID"]))
                .Where((OOQL.AuthFilter("MO_ROUTING", "MO_ROUTING"))
                    & (OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(_paraObject.site_no)
                      & OOQL.CreateProperty("MO.DOC_NO") == OOQL.CreateConstants(_paraObject.wo_no)
                      & OOQL.CreateProperty("MO_ROUTING_D.OPERATION_SEQ") == OOQL.CreateConstants(_paraObject.op_seq)
                      & OOQL.CreateProperty("OPERATION.OPERATION_CODE") == OOQL.CreateConstants(_paraObject.op_no)));

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            if (result.Count > 0)
                _queryMoRouting = result[0];
        }

        /// <summary>
        /// 投产逻辑
        /// </summary>
        private void MoveIn(DependencyObjectCollection rtnColl) {
            IMoGIOService moGioSrv = this.GetServiceForThisTypeKey<IMoGIOService>();
            MoGIOEntity entity = new MoGIOEntity(_queryMoRouting["INSPECT_MODE"].ToStringExtension(), _paraObject.reports_qty
                , _queryMoRouting["PRE_PROCESSING_UNIT_ID"], _queryMoRouting["MO_ROUTING_D_ID"], _queryWorkCenter["DISPATCH_CONTROL"].ToBoolean());
            entity.ColCHARACTER = "WORK_CENTER";
            entity.ColWORK_CENTER_ID = _queryWorkCenter["WORK_CENTER_ID"];
            entity.ColPLANT_ID = _queryMoRouting["PLANT_ID"];
            Collection<MoGIOEntity> collection = new Collection<MoGIOEntity>();
            collection.Add(entity);

            RtMoGIOEntity result = moGioSrv.DoMoGIO(collection);
            if (result != null && result.OResult) {
                SetValue(rtnColl, 0, "", "", "");
            } else {
                throw new BusinessRuleException(result.OMsg);
            }
        }

        /// <summary>
        /// 上线逻辑
        /// </summary>
        /// <param name="rtnColl"></param>
        private void CheckIn(DependencyObjectCollection rtnColl) {
            #region 20170801 mark by shenbao for P001-170717001 由于其他地方要用，所以将下面的提取成方法
            //bool paraCheckOut = false;
            //IQueryService qrySrv = this.GetService<IQueryService>();
            
            ////由于查询优先级不一样，以下查询不可以合并
            //QueryNode node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL")
            //    .From("PARA_OP_TRACK", "PARA_OP_TRACK")
            //    .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
            //        & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
            //            & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(_queryWorkCenter["WORK_CENTER_ID"])
            //            & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(_queryMoRouting["OPERATION_ID"])));
            //DependencyObjectCollection result = qrySrv.ExecuteDependencyObject(node);
            //if (result.Count == 0) {
            //    node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL")
            //    .From("PARA_OP_TRACK", "PARA_OP_TRACK")
            //    .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
            //        & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
            //            & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(_queryWorkCenter["WORK_CENTER_ID"])
            //            & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())));
            //    result = qrySrv.ExecuteDependencyObject(node);
            //    if (result.Count == 0) {
            //        node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL")
            //    .From("PARA_OP_TRACK", "PARA_OP_TRACK")
            //    .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
            //        & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
            //            & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())
            //            & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(_queryMoRouting["OPERATION_ID"])));
            //        result = qrySrv.ExecuteDependencyObject(node);
            //    }
            //}

            //if (result.Count > 0)
            //    paraCheckOut = result[0]["CHECKOUT_CONTROL"].ToBoolean();
            //_paraCheckOut = paraCheckOut;  //20170727 add by shenbao for P001-170717001 记录下来，后续还要使用
            #endregion

            ICheckInService checkInSrv = this.GetServiceForThisTypeKey<ICheckInService>();
            CheckInParamters para = new CheckInParamters("WORK_CENTER", _queryMoRouting["MO_ROUTING_D_ID"], _queryWorkCenter["WORK_CENTER_ID"]
                , _queryMoRouting["PRE_PROCESSING_UNIT_ID"], _paraObject.reports_qty, _queryMoRouting["INSPECT_MODE"].ToStringExtension());
            para.CheckOutControl = _paraCheckOut;  //20170801 modi by shenbao for P001-170717001
            para.Remark = "";
            para.PlantId = _queryMoRouting["PLANT_ID"];
            List<CheckInParamters> collection = new List<CheckInParamters>();
            collection.Add(para);

            RtCheckIn rtn = checkInSrv.CheckIn(collection);
            if (rtn != null && rtn.OResult) {
                SetValue(rtnColl, 0, "", "", "");
            } else {
                throw new BusinessRuleException(string.Join("\r\n", rtn.OErrorMsgList.Select(c => c.OMsg).ToArray()));
            }
        }

        //20170801 add by shenbao for P001-170717001
        private void QueryCheckOutControl() {
            IQueryService qrySrv = this.GetService<IQueryService>();
            //由于查询优先级不一样，以下查询不可以合并
            QueryNode node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL",
                    "PARA_OP_TRACK.CHECKIN_CONTROL"
                )
                .From("PARA_OP_TRACK", "PARA_OP_TRACK")
                .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
                    & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                        & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(_queryWorkCenter["WORK_CENTER_ID"])
                        & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(_queryMoRouting["OPERATION_ID"])));
            DependencyObjectCollection result = qrySrv.ExecuteDependencyObject(node);
            if (result.Count == 0) {
                node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL",
                        "PARA_OP_TRACK.CHECKIN_CONTROL"
                    )
                .From("PARA_OP_TRACK", "PARA_OP_TRACK")
                .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
                    & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                        & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(_queryWorkCenter["WORK_CENTER_ID"])
                        & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())));
                result = qrySrv.ExecuteDependencyObject(node);
                if (result.Count == 0) {
                    node = OOQL.Select("PARA_OP_TRACK.CHECKOUT_CONTROL",
                            "PARA_OP_TRACK.CHECKIN_CONTROL"
                        )
                .From("PARA_OP_TRACK", "PARA_OP_TRACK")
                .Where((OOQL.AuthFilter("PARA_OP_TRACK", "PARA_OP_TRACK"))
                    & (OOQL.CreateProperty("PARA_OP_TRACK.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                        & OOQL.CreateProperty("PARA_OP_TRACK.WORK_CENTER_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())
                        & OOQL.CreateProperty("PARA_OP_TRACK.OPERATION_ID") == OOQL.CreateConstants(_queryMoRouting["OPERATION_ID"])));
                    result = qrySrv.ExecuteDependencyObject(node);
                }
            }

            if (result.Count > 0) {
                _paraCheckOut = result[0]["CHECKOUT_CONTROL"].ToBoolean();
                _paraCheckIn = result[0]["CHECKIN_CONTROL"].ToBoolean();
            }
        }

        /// <summary>
        /// 下线逻辑
        /// </summary>
        /// <param name="rtnColl"></param>
        private void CheckOut(DependencyObjectCollection rtnColl) {
            ICheckOutService checkOutSrv = this.GetServiceForThisTypeKey<ICheckOutService>();
            CheckOutParamters para = new CheckOutParamters("WORK_CENTER", _queryMoRouting["MO_ROUTING_D_ID"], _queryWorkCenter["WORK_CENTER_ID"]
                , _queryMoRouting["PRE_PROCESSING_UNIT_ID"], _paraObject.reports_qty, _queryMoRouting["INSPECT_MODE"].ToStringExtension());
            para.Remark = "";
            para.PlantId = _queryMoRouting["PLANT_ID"];
            List<CheckOutParamters> collection = new List<CheckOutParamters>();
            collection.Add(para);

            RtCheckOut rtn = checkOutSrv.CheckOut(collection);
            if (rtn != null && rtn.OResult) {
                SetValue(rtnColl, 0, "", "", "");
            } else {
                throw new BusinessRuleException(string.Join("\r\n", rtn.OErrorMsgList.Select(c => c.OMsg).ToArray()));
            }
        }

        /// <summary>
        /// 转移逻辑
        /// </summary>
        /// <param name="rtnColl"></param>
        private void MoveOut(DependencyObjectCollection rtnColl, string reportType) {
            //一次查询关联比较多，不见得比分开且关联表较少性能快，所以分开查询（且相比规格已经做了查询次数的优化）
            QueryDoc();
            if (_queryDoc == null)
                throw new BusinessRuleException(EncodeSrv.GetMessage("A111275"));  //20161213 modi by shenbao for B001-161213006
            QueryDataInfo();
            if (_queryDataInfo == null)
                throw new BusinessRuleException("QueryMoProduct");
            QueryMoRoutingPath();
            if (_queryMoRoutingPath == null)
                throw new BusinessRuleException("QueryMoRoutingPath");

            DependencyObjectCollection warehouseBinColl = QueryWarehouseBin();//20170406 add by wangyq for P001-170327001 
            //生成转移单
            ICreateService createSrv = this.GetService<ICreateService>("WIP_TRANSFER_DOC");
            DependencyObject wipTransferDoc = createSrv.Create() as DependencyObject;
            CreateTransferDoc(wipTransferDoc, warehouseBinColl, reportType);

            //保存转移单
            ISaveService saveSrv = this.GetService<ISaveService>("WIP_TRANSFER_DOC");
            //20170619 modi by zhangcn for P001-170606002 ===begin===
            try {
                SetIgnoreWarningTag(); //忽略警告
                saveSrv.Save(wipTransferDoc);
            }
            finally {
                ResetIgnoreWarningTag();// 重置警告
            }
            //20170619 modi by zhangcn for P001-170606002 ===end===


            //事后收集
            bool isCollectTime = SysParaSrv.GetValue("COLLECT_TIME", _queryMoRouting["PLANT_ID"]).ToBoolean();
            if (isCollectTime) {
                //单身集合，一定会有一笔记录
                //20170801 mark by shenbao for P001-170717001 ===begin===
                ////20170727 add by shenbao for P001-170717001 ===begin===
                //int manHour = (_paraCheckIn && _paraCheckOut && _paraObject.labor_hours == 0) ? _queryMoRouting["STANDARD_MAN_HOUR"].ToInt32() : _paraObject.labor_hours;  
                //int machineHour = (_paraCheckIn && _paraCheckOut && _paraObject.machine_hours == 0) ? _queryMoRouting["STANDARD_MACHINE_HOUR"].ToInt32() : _paraObject.machine_hours;
                ////20170727 add by shenbao for P001-170717001 ===end===
                //20170801 mark by shenbao for P001-170717001 ===end===

                //20170801 add by shenbao for P001-170717001 ===begin===
                int manHour = 0;  //人时
                int machineHour = 0;  //机时
                if (_paraCheckOut && _paraCheckIn && _paraObject.labor_hours == 0) {
                    manHour = (_queryMoRouting["LAST_CHECKOUT_DATETIME"].ToDate() - _queryMoRouting["FIRST_CHECKIN_TIME"].ToDate()).TotalSeconds.ToInt32();
                } else if (_paraObject.labor_hours != 0) {
                    manHour = _paraObject.labor_hours * 60;
                } else {
                    manHour = _queryMoRouting["STANDARD_MAN_HOUR"].ToInt32();
                }

                if (_paraCheckOut && _paraCheckIn && _paraObject.machine_hours == 0) {
                    machineHour = (_queryMoRouting["LAST_CHECKOUT_DATETIME"].ToDate() - _queryMoRouting["FIRST_CHECKIN_TIME"].ToDate()).TotalSeconds.ToInt32();
                } else if (_paraObject.machine_hours != 0) {
                    machineHour = _paraObject.machine_hours * 60;
                } else {
                    machineHour = _queryMoRouting["STANDARD_MACHINE_HOUR"].ToInt32();
                }
                //20170801 add by shenbao for P001-170717001 ===end===

                DependencyObjectCollection wipLine = wipTransferDoc["WIP_TRANSFER_DOC_D"] as DependencyObjectCollection;
                InsertDataCollectParam para = new InsertDataCollectParam(1, _queryMoRouting["MO_ID"], _queryDataInfo["ITEM_ID"]
                    , _queryDataInfo["ITEM_FEATURE_ID"], _queryDataInfo["ITEM_NAME"].ToStringExtension()
                    , _queryDataInfo["ITEM_SPECIFICATION"].ToStringExtension(), _queryMoRouting["MO_ROUTING_D_ID"]
                    , _queryMoRouting["OPERATION_ID"], _paraObject.reports_qty, _paraObject.scrap_qty, manHour   //20170727 modi by shenbao for P001-170717001 修改人时机时
                    , machineHour, wipLine[0].ExtendedProperties["uiSOURCE_ID_RTK"].ToStringExtension()
                    , wipLine[0].ExtendedProperties["uiSOURCE_ID_ROid"], _queryDataInfo["WORK_TEAM_ID"], _queryMoRouting["AFTER_PROCESSING_UNIT_ID"]
                    , _queryDataInfo["EMPLOYEE_ID"], wipLine[0].Oid, _queryMoRouting["LABOR_RATIO"].ToDecimal(), "1", Maths.GuidDefaultValue());  //20170612 modi by shenbao for P001-170606002
                Collection<InsertDataCollectParam> collection = new Collection<InsertDataCollectParam>();
                collection.Add(para);

                IInsertDataCollectService insertDataColl = this.GetService<IInsertDataCollectService>("WIP_TRANSFER_DOC");
                insertDataColl.insertDataCollect(_queryMoRouting["PLANT_ID"], _queryWorkCenter["Owner_Dept"], _queryDataInfo["EMPLOYEE_ID"]
                    , DateTime.Now, _queryWorkCenter["WORK_CENTER_ID"], wipTransferDoc.Oid, collection);
            }

            SetValue(rtnColl, 0, "", "", wipTransferDoc["DOC_NO"].ToStringExtension());
        }

        /// <summary>
        /// 创建转移单
        /// </summary>
        /// <param name="wipTransferDoc">转移单</param>
        private void CreateTransferDoc(DependencyObject wipTransferDoc, DependencyObjectCollection warehouseBinColl, string reportType) {
            #region 单头赋值
            //组织
            DependencyObject org = wipTransferDoc["Owner_Org"] as DependencyObject;
            org["RTK"] = "PLANT";
            org["ROid"] = _queryMoRouting["PLANT_ID"];
            wipTransferDoc["DOC_ID"] = _queryDoc["DOC_ID"];  //单据类型
            wipTransferDoc["DOC_NO"] = DocumentNumberGenSrv.NextNumber(_queryDoc["DOC_ID"], DateTime.Now);  //单号
            wipTransferDoc["DOC_DATE"] = DateTime.Now.Date;  //单据日期
            //移出类别
            DependencyObject sourceID = wipTransferDoc["SOURCE_ID"] as DependencyObject;
            sourceID["RTK"] = "WORK_CENTER";
            sourceID["ROid"] = _queryWorkCenter["WORK_CENTER_ID"];
            wipTransferDoc["FROM_ADMIN_UNIT_ID"] = _queryWorkCenter["Owner_Dept"];//移出部门
            wipTransferDoc["Owner_Emp"] = _queryDataInfo["EMPLOYEE_ID"];//移转人员
            wipTransferDoc["Owner_Dept"] = _queryWorkCenter["Owner_Dept"];//移转部门
            wipTransferDoc["TRANSACTION_DATE"] = DateTime.Now.Date;  //转移日期
            wipTransferDoc["CATEGORY"] = _queryDoc["CATEGORY"];  //单据性质码
            wipTransferDoc["REMARK"] = "";//备注
            wipTransferDoc["PURCHASE_RECEIPT_ID"] = Maths.GuidDefaultValue();  //采购入库单
            #endregion

            //创建单身
            //单身也只有一笔记录
            CreateTransferDocLine(wipTransferDoc, warehouseBinColl, reportType);
        }

        private void CreateTransferDocLine(DependencyObject wipTransferDoc, DependencyObjectCollection warehouseBinColl, string reportType) {
            //只有一笔单身
            DependencyObjectCollection entityD = wipTransferDoc["WIP_TRANSFER_DOC_D"] as DependencyObjectCollection;
            DependencyObject line = entityD.AddNew();
            #region 单身赋值

            line["SequenceNumber"] = 1;  //序号
            line["MO_ID"] = _queryMoRouting["MO_ID"];  //工单ID
            line["TYPE"] = "1";  //类型
            line["PRODUCT_TYPE"] = _queryDataInfo["PRODUCT_TYPE"];  //产出类型
            line["ITEM_ID"] = _queryDataInfo["ITEM_ID"];  //品号
            line["ITEM_NAME"] = _queryDataInfo["ITEM_NAME"];  //品名
            line["ITEM_SPECIFICATION"] = _queryDataInfo["ITEM_SPECIFICATION"];//产品规格
            line["ITEM_FEATURE_ID"] = _queryDataInfo["ITEM_FEATURE_ID"];  //特征码
            line["FROM_MO_ROUTING_D_ID"] = _queryMoRouting["MO_ROUTING_D_ID"];  //移出工序
            //移入类别
            DependencyObject sourceID = line["SOURCE_ID"] as DependencyObject;
            if (reportType == "6")//20170406 modi by wangyq for P001-170327001 old:if (Maths.IsEmpty(_queryMoRoutingPath["MO_ROUTING_D_ID"]))
                sourceID["RTK"] = "WAREHOUSE";
            else
                sourceID["RTK"] = _queryMoRoutingPath["SOURCE_ID_RTK"];
            //移入地
            if (reportType == "6")//20170406 modi by wangyq for P001-170327001 old:if (Maths.IsEmpty(_queryMoRoutingPath["MO_ROUTING_D_ID"]))
                sourceID["ROid"] = warehouseBinColl.Count > 0 ? warehouseBinColl[0]["WAREHOUSE_ID"] : Maths.GuidDefaultValue();//20170406 modi by wangyq for P001-170327001 old:_queryDataInfo["INBOUND_WAREHOUSE_ID"];
            else
                sourceID["ROid"] = _queryMoRoutingPath["SOURCE_ID_ROid"];
            //移入部门
            if (_queryMoRoutingPath["SOURCE_ID_RTK"].ToStringExtension() == "WORK_CENTER")
                line["TO_ADMIN_UNIT_ID"] = _queryMoRoutingPath["Owner_Dept"];
            else
                line["TO_ADMIN_UNIT_ID"] = Maths.GuidDefaultValue();
            //移入工序
            if (Maths.IsEmpty(_queryMoRoutingPath["MO_ROUTING_D_ID"]))
                line["TO_MO_ROUTING_D_ID"] = Maths.GuidDefaultValue();
            else
                line["TO_MO_ROUTING_D_ID"] = _queryMoRoutingPath["MO_ROUTING_D_ID"];
            line["QTY"] = _paraObject.reports_qty;//数量
            line["BONUS_QTY"] = 0;  //超入量
            line["SCRAP_QTY"] = _paraObject.scrap_qty;//报废数量
            line["DESTROYED_QTY"] = 0;//破坏数量
            line["UNIT_ID"] = _queryMoRouting["AFTER_PROCESSING_UNIT_ID"];//单位
            //采购单别
            if (_queryDoc["AUTO_PO"].ToBoolean()
                && _queryMoRoutingPath["SOURCE_ID_RTK"].ToStringExtension() == "SUPPLIER") {
                line["PURCHASE_DOC_ID"] = _queryDoc["PURCHASE_DOC_ID"];
            } else
                line["PURCHASE_DOC_ID"] = Maths.GuidDefaultValue();
            //库位

            if (reportType == "6" && !string.IsNullOrEmpty(_paraObject.storage_spaces_no)) {//20170406 modi by wangyq for P001-170327001 old:
                //if (Maths.IsEmpty(_queryMoRoutingPath["MO_ROUTING_D_ID"])
                //&& Maths.IsNotEmpty(_queryDataInfo["BIN_ID"])) {
                line["BIN_ID"] = warehouseBinColl.Count > 0 ? warehouseBinColl[0]["BIN_ID"] : Maths.GuidDefaultValue();//20170406 modi by wangyq for P001-170327001 old:_queryDataInfo["BIN_ID"];
            } else
                line["BIN_ID"] = Maths.GuidDefaultValue();
            //批号
            line["ITEM_LOT_ID"] = _queryDataInfo["ITEM_LOT_ID"];
            //最早上线时间
            if (Maths.IsEmpty(_queryMoRouting["FIRST_CHECKIN_TIME"]))
                line["FIRST_CHECKIN_TIME"] = DateTime.Now;
            else
                line["FIRST_CHECKIN_TIME"] = _queryMoRouting["FIRST_CHECKIN_TIME"];
            //最晚下线时间
            if (Maths.IsMaxDateTime(_queryMoRouting["LAST_CHECKOUT_DATETIME"]))
                line["LAST_CHECKOUT_DATETIME"] = DateTime.Now;
            else
                line["LAST_CHECKOUT_DATETIME"] = _queryMoRouting["LAST_CHECKOUT_DATETIME"];
            //备注
            line["REMARK"] = "";
            //机器类型
            if (Maths.IsNotEmpty(_queryDataInfo["MACHINE_TEAM_ID"]))
                line.ExtendedProperties.Add("uiSOURCE_ID_RTK", "MACHINE_TEAM");
            else
                line.ExtendedProperties.Add("uiSOURCE_ID_RTK", "MACHINE");
            //机器
            if (Maths.IsNotEmpty(_queryDataInfo["MACHINE_TEAM_ID"]))
                line.ExtendedProperties.Add("uiSOURCE_ID_ROid", _queryDataInfo["MACHINE_TEAM_ID"]);
            else
                line.ExtendedProperties.Add("uiSOURCE_ID_ROid", _queryDataInfo["MACHINE_ID"]);
            //班组
            line.ExtendedProperties.Add("uiWORK_TEAM_ID", _queryDataInfo["WORK_TEAM_ID"]);
            //人员
            line.ExtendedProperties.Add("uiEMPLOYEE_ID", _queryDataInfo["EMPLOYEE_ID"]);
            //20170801 mark by shenbao for P001-170717001 ===begin===
            ////使用人时
            //line.ExtendedProperties.Add("uiMAN_HOUR", _paraObject.labor_hours * 60);
            ////使用机时
            //line.ExtendedProperties.Add("uiMACHINE_HOUR", _paraObject.machine_hours * 60);
            //20170801 mark by shenbao for P001-170717001 ===end===
            //20170801 add by shenbao for P001-170717001 ===begin===
            int manHour = 0;  //人时
            int machineHour = 0;  //机时
            if (_paraCheckOut && _paraCheckIn && _paraObject.labor_hours == 0) {
                manHour = (_queryMoRouting["LAST_CHECKOUT_DATETIME"].ToDate() - _queryMoRouting["FIRST_CHECKIN_TIME"].ToDate()).TotalSeconds.ToInt32();
            } else if (_paraObject.labor_hours != 0) {
                manHour = _paraObject.labor_hours * 60;
            } else {
                manHour = _queryMoRouting["STANDARD_MAN_HOUR"].ToInt32();
            }

            if (_paraCheckOut && _paraCheckIn && _paraObject.machine_hours == 0) {
                machineHour = (_queryMoRouting["LAST_CHECKOUT_DATETIME"].ToDate() - _queryMoRouting["FIRST_CHECKIN_TIME"].ToDate()).TotalSeconds.ToInt32();
            } else if (_paraObject.machine_hours != 0) {
                machineHour = _paraObject.machine_hours * 60;
            } else {
                machineHour = _queryMoRouting["STANDARD_MACHINE_HOUR"].ToInt32();
            }
            //使用人时
            line.ExtendedProperties.Add("uiMAN_HOUR", manHour);
            //使用机时
            line.ExtendedProperties.Add("uiMACHINE_HOUR", machineHour);
            //20170801 add by shenbao for P001-170717001 ===end===
            //工艺
            line.ExtendedProperties.Add("uiOPERATION_ID", _queryMoRouting["OPERATION_ID"]);
            //工时工资率
            line.ExtendedProperties.Add("uiLABOR_RATIO", _queryMoRouting["LABOR_RATIO"]);
            //计件类型
            line.ExtendedProperties.Add("uiPIECE_TYPE", "1");
            //20170619 add by zhangcn for P001-170606002===begin===
            DependencyObject sourceDocID = line["SOURCE_DOC_ID"] as DependencyObject;
            sourceDocID["RTK"] = "OTHER";
            sourceDocID["ROid"] = Maths.GuidDefaultValue();
            //20170619 add by zhangcn for P001-170606002===end===
            #endregion

            //创建子单身
            CreateTransferDocSubLine(line);
        }

        /// <summary>
        /// 创建子单身
        /// </summary>
        /// <param name="line">单身</param>
        private void CreateTransferDocSubLine(DependencyObject line) {
            DependencyObjectCollection subColl = QueryWorkTeamD();

            DependencyObjectCollection wipSubLineColl = line["WIP_TRANSFER_DOC_SD"] as DependencyObjectCollection;
            foreach (DependencyObject item in subColl) {
                DependencyObject subLine = wipSubLineColl.AddNew();

                #region 子单身赋值

                decimal allocationWeights = item["ALLOCATION_WEIGHTS"].ToDecimal();
                //生产资料收集单身
                subLine["SF_DATA_COLLECT_D_ID"] = Maths.GuidDefaultValue();
                //分摊比率
                subLine["ALLOCATION_WEIGHTS"] = item["ALLOCATION_WEIGHTS"];
                //数量
                subLine["WIP_TRANSFER_QTY"] = line["QTY"].ToDecimal() * allocationWeights;
                //使用人时
                subLine["MAN_HOUR"] = Convert.ToInt32((line.ExtendedProperties["uiMAN_HOUR"].ToInt32() * allocationWeights), CultureInfo.CurrentCulture);  //附：这里不能使用ToInt32的扩展函数，因为其内部实现用TryPars，对于1.5这样的小数是转换不成功的，始终返回0
                //使用机时
                subLine["MACHINE_HOUR"] = Convert.ToInt32(line.ExtendedProperties["uiMACHINE_HOUR"].ToInt32() * allocationWeights, CultureInfo.CurrentCulture);
                //机器类型
                DependencyObject sourceID = subLine["SOURCE_ID"] as DependencyObject;
                sourceID["RTK"] = line.ExtendedProperties["uiSOURCE_ID_RTK"];
                sourceID["ROid"] = line.ExtendedProperties["uiSOURCE_ID_ROid"];
                //人员
                subLine["EMPLOYEE_ID"] = item["EMPLOYEE_ID"];
                //金额
                subLine["AMT"] = line["QTY"].ToDecimal() * allocationWeights * _queryMoRouting["PIECE_PRICE"].ToDecimal();

                #endregion
            }
        }

        /// <summary>
        /// 查询单据类型信息
        /// </summary>
        private void QueryDoc() {
            QueryNode node = OOQL.Select(1, "PARA_DOC_FIL.DOC_ID"
                    , "DOC.SEQUENCE_DIGIT"  //流水号位数
                    , "AUTO_PO"  //委外时产生采购订单
                    , "PURCHASE_DOC_ID"  //采购单据类型
                    , "DOC.CATEGORY"  //单据性质
                )
                .From("PARA_DOC_FIL", "PARA_DOC_FIL")
                .InnerJoin("DOC")
                .On(OOQL.CreateProperty("PARA_DOC_FIL.DOC_ID") == OOQL.CreateProperty("DOC.DOC_ID"))
                .Where((OOQL.AuthFilter("PLANT", "PLANT"))
                    & (OOQL.CreateProperty("PARA_DOC_FIL.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                        & ((OOQL.CreateConstants(_queryMoRouting["POSITION_FLAG"].ToStringExtension()).In(OOQL.CreateConstants("0"), OOQL.CreateConstants("2"))
                            & OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants("5B"))
                           | (OOQL.CreateConstants(_queryMoRouting["POSITION_FLAG"].ToStringExtension()).In(OOQL.CreateConstants("1"), OOQL.CreateConstants("3"))
                            & OOQL.CreateProperty("PARA_DOC_FIL.CATEGORY") == OOQL.CreateConstants("5C")))
                        & (OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(_queryMoRouting["DOC_ID"])  //20170223 add by shenbao for P001-170221002
                            | OOQL.CreateProperty("PARA_DOC_FIL.SOURCE_DOC_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))))
                .OrderBy(new OrderByItem[] { OOQL.CreateOrderByItem("PARA_DOC_FIL.SOURCE_DOC_ID", SortType.Desc) });  //20170223 add by shenbao for P001-170221002

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            if (result.Count > 0)
                _queryDoc = result[0];
        }

        /// <summary>
        /// 查询新增所需要的数据信息
        /// 只有一笔记录
        /// 改方法集合了规格中的QueryMoProduct、QueryItemLot、QueryMachine、QueryMachineTeam、QueryWorkTeam、QueryEmployee
        /// 因为其中的每一种查询都最多只有一笔记录
        /// </summary>
        private void QueryDataInfo() {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("MO_PRODUCT.ITEM_DESCRIPTION", "ITEM_NAME") //品名
                    , OOQL.CreateProperty("MO_PRODUCT.ITEM_SPECIFICATION")  //规格
                    , OOQL.CreateProperty("MO_PRODUCT.PRODUCT_TYPE")  //产出类型
                    , OOQL.CreateProperty("MO_PRODUCT.ITEM_ID")  //品号
                    , OOQL.CreateProperty("MO_PRODUCT.ITEM_FEATURE_ID")  //特征码
                    , Formulas.IsNull(OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())
                        , "INBOUND_WAREHOUSE_ID")  //入库仓库
                    , Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())
                        , "BIN_ID")  //库位
                    , Formulas.IsNull(OOQL.CreateProperty("ITEM_LOT.ITEM_LOT_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())
                        , "ITEM_LOT_ID")  //批号
                    , Formulas.IsNull(OOQL.CreateProperty("WORK_TEAM.WORK_TEAM_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())
                        , "WORK_TEAM_ID")  //班组
                    , Formulas.IsNull(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue())
                        , "EMPLOYEE_ID")  //人员
                    , OOQL.CreateProperty("MACHINE.MACHINE_ID")//机器
                    , OOQL.CreateProperty("MACHINE_TEAM.MACHINE_TEAM_ID")//机器组
                )
                .From("MO.MO_PRODUCT", "MO_PRODUCT")
                .InnerJoin("ITEM")
                .On(OOQL.CreateProperty("MO_PRODUCT.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID"))
                .InnerJoin("ITEM_PLANT")
                .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_PLANT.ITEM_ID")
                    & OOQL.CreateProperty("ITEM_PLANT.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"]))
                .LeftJoin("WAREHOUSE.BIN", "BIN")
                .On(OOQL.CreateProperty("ITEM_PLANT.INBOUND_WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                    & OOQL.CreateProperty("BIN.MAIN") == OOQL.CreateConstants(true))
                .LeftJoin("ITEM_LOT")
                .On(OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(_paraObject.lot_no)
                    & OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_LOT.ITEM_ID")
                    & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateProperty("MO_PRODUCT.ITEM_FEATURE_ID"))
                .LeftJoin("MACHINE")
                .On(OOQL.CreateProperty("MACHINE.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                    & OOQL.CreateProperty("MACHINE.MACHINE_CODE") == OOQL.CreateConstants(_paraObject.machine_no))
                .LeftJoin("MACHINE_TEAM")
                .On(OOQL.CreateProperty("MACHINE_TEAM.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                    & OOQL.CreateProperty("MACHINE_TEAM.TEAM_CODE") == OOQL.CreateConstants(_paraObject.machine_no))
                .LeftJoin("WORK_TEAM")
                .On(OOQL.CreateProperty("WORK_TEAM.Owner_Org.ROid") == OOQL.CreateConstants(_queryMoRouting["PLANT_ID"])
                    & OOQL.CreateProperty("WORK_TEAM.WORK_TEAM_CODE") == OOQL.CreateConstants(_paraObject.shift_no))
                .LeftJoin("EMPLOYEE")
                .On(OOQL.CreateProperty("EMPLOYEE.EMPLOYEE_CODE") == OOQL.CreateConstants(_paraObject.employee_no))
                .Where((OOQL.AuthFilter("MO.MO_PRODUCT", "MO_PRODUCT"))  //20161208 MODI by shenbao fro P001-161208001
                    & (OOQL.CreateProperty("MO_PRODUCT.MO_ID") == OOQL.CreateConstants(_queryMoRouting["MO_ID"])
                    & OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateConstants(_paraObject.item_no)));

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            if (result.Count > 0)
                _queryDataInfo = result[0];
        }

        /// <summary>
        /// 后工序
        /// </summary>
        private void QueryMoRoutingPath() {
            QueryNode node = OOQL.Select(1, OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.SOURCE_ID.RTK", "SOURCE_ID_RTK") //性质
                    , OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.SOURCE_ID.ROid", "SOURCE_ID_ROid")  //工作中心
                    , OOQL.CreateProperty("WORK_CENTER.Owner_Dept")  //部门
                    , OOQL.CreateProperty("AFTER_MO_ROUTING_D.MO_ROUTING_D_ID")  //后工序
                )
                .From("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D")
                .InnerJoin("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_PATH", "MO_ROUTING_PATH")
                .On(OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("MO_ROUTING_PATH.MO_ROUTING_D_ID")
                    & OOQL.CreateProperty("MO_ROUTING_PATH.PATH_TYPE") == OOQL.CreateConstants("0"))
                .LeftJoin("MO_ROUTING.MO_ROUTING_D", "AFTER_MO_ROUTING_D")
                .On(OOQL.CreateProperty("MO_ROUTING_PATH.TO_SEQ") == OOQL.CreateProperty("AFTER_MO_ROUTING_D.OPERATION_SEQ")
                    & OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_ID") == OOQL.CreateProperty("AFTER_MO_ROUTING_D.MO_ROUTING_ID"))
                .LeftJoin("MO_ROUTING.MO_ROUTING_D.MO_ROUTING_WORK_CENTER", "MO_ROUTING_WORK_CENTER")
                .On(OOQL.CreateProperty("AFTER_MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.MO_ROUTING_D_ID")
                    & OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.MAIN_STATION") == OOQL.CreateConstants(true))
                .LeftJoin("WORK_CENTER")
                .On(OOQL.CreateProperty("MO_ROUTING_WORK_CENTER.SOURCE_ID.ROid") == OOQL.CreateProperty("WORK_CENTER.WORK_CENTER_ID"))
                .Where((OOQL.AuthFilter("MO_ROUTING.MO_ROUTING_D", "MO_ROUTING_D"))
                    & (OOQL.CreateProperty("MO_ROUTING_D.MO_ROUTING_D_ID") == OOQL.CreateConstants(_queryMoRouting["MO_ROUTING_D_ID"])));

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            if (result.Count > 0)
                _queryMoRoutingPath = result[0];
        }

        /// <summary>
        /// 查询班组单身信息
        /// </summary>
        /// <param name="workTeamID"></param>
        /// <returns></returns>
        private DependencyObjectCollection QueryWorkTeamD() {
            QueryNode node = OOQL.Select("WORK_TEAM_D.EMPLOYEE_ID", "WORK_TEAM_D.ALLOCATION_WEIGHTS")
                .From("WORK_TEAM.WORK_TEAM_D", "WORK_TEAM_D")
                .Where((OOQL.AuthFilter("WORK_TEAM.WORK_TEAM_D", "WORK_TEAM_D"))
                    & (OOQL.CreateProperty("WORK_TEAM_D.WORK_TEAM_ID") == OOQL.CreateConstants(_queryDataInfo["WORK_TEAM_ID"])));

            DependencyObjectCollection result = this.GetService<IQueryService>().ExecuteDependencyObject(node);

            return result;
        }

        //20170406 add by wangyq for P001-170327001 ==========begin==============
        private DependencyObjectCollection QueryWarehouseBin() {
            QueryNode node = OOQL.Select(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID")
                                     , Formulas.IsNull(OOQL.CreateProperty("BIN.BIN_ID"), OOQL.CreateConstants(Maths.GuidDefaultValue()), "BIN_ID"))
                            .From("WAREHOUSE", "WAREHOUSE")
                            .InnerJoin("PLANT", "PLANT")
                            .On(OOQL.CreateProperty("WAREHOUSE.Owner_Org.ROid") == OOQL.CreateProperty("PLANT.PLANT_ID"))
                            .LeftJoin("WAREHOUSE.BIN", "BIN")
                            .On(OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_ID") == OOQL.CreateProperty("BIN.WAREHOUSE_ID")
                                & OOQL.CreateProperty("BIN.BIN_CODE") == OOQL.CreateConstants(_paraObject.storage_spaces_no))
                            .Where(OOQL.AuthFilter("WAREHOUSE", "WAREHOUSE")
                                   & OOQL.CreateProperty("WAREHOUSE.WAREHOUSE_CODE") == OOQL.CreateConstants(_paraObject.warehouse_no)
                                   & OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(_paraObject.site_no));
            return this.GetService<IQueryService>().ExecuteDependencyObject(node);
        }
        //20170406 add by wangyq for P001-170327001 ==========end==============
        /// <summary>
        /// 释放内存
        /// </summary>
        private void ClearData() {
            _paraObject = null;
            _queryWorkCenter = null;
            _queryMoRouting = null;
            _queryDoc = null;
            _queryDataInfo = null;
            _queryMoRoutingPath = null;
            _documentNumberGenSrv = null;
            _encodeSrv = null;
            _sysParaSrv = null;
        }

        #endregion

        #region 20170619 add by zhangcn for P001-170606002
        private void SetIgnoreWarningTag() {
            DeliverContext deliver = CallContext.GetData(DeliverContext.Name) as DeliverContext;
            if (deliver == null) {
                deliver = new DeliverContext();
                CallContext.SetData(DeliverContext.Name, deliver);
            }
            if (deliver.ContainsKey("IgnoreWarning")) {
                deliver["IgnoreWarning"] = true;
            }
            else {
                deliver.Add("IgnoreWarning", true);
            }
        }

        private void ResetIgnoreWarningTag() {
            DeliverContext deliver = CallContext.GetData(DeliverContext.Name) as DeliverContext;
            if (deliver != null && deliver.ContainsKey("IgnoreWarning")) {
                deliver["IgnoreWarning"] = false;
            }
        }

        #endregion
    }

    /// <summary>
    /// 由于该接口参数太多，导致后续参数传递可以也跟着多
    /// 组建对象存储，方便后续调用
    /// </summary>
    public class ParaObject {
        #region properties
        //为了保证属性名字和接口对应，方便后续维护，就不再遵循属性明名规范

        public string employee_no { get; set; }
        public string report_type { get; set; }
        public string wo_no { get; set; }
        public string run_card_no { get; set; }
        public string op_no { get; set; }
        public string op_seq { get; set; }
        public string workstation_no { get; set; }
        public string machine_no { get; set; }
        public string shift_no { get; set; }
        public int labor_hours { get; set; }
        public int machine_hours { get; set; }
        public decimal reports_qty { get; set; }
        public decimal scrap_qty { get; set; }
        public string item_no { get; set; }
        public string site_no { get; set; }
        public string abnormal_no { get; set; }
        public decimal defect_qty { get; set; }
        public int seq { get; set; }
        public string component_item_no { get; set; }
        public decimal qty { get; set; }
        public string lot_no { get; set; }

        //20170406 add by wangyq for P001-170327001 ==========begin==============
        /// <summary>
        /// 仓库
        /// </summary>
        public string warehouse_no { get; set; }
        /// <summary>
        /// 库位
        /// </summary>
        public string storage_spaces_no { get; set; }
        //20170406 add by wangyq for P001-170327001 ==========end==============

        #endregion

        #region ctor
        public ParaObject(string employee_no, string report_type, string wo_no
            , string run_card_no, string op_no, string op_seq, string workstation_no, string machine_no
            , string shift_no, int labor_hours, int machine_hours, decimal reports_qty
            , decimal scrap_qty, string item_no, string site_no
            , string lot_no, string warehouse_no, string storage_spaces_no//20170406 add by wangyq for P001-170327001 先补上lot_no，其他比规格缺少的字段跟sd确认后是无用的,无需添加
            ) {
            this.employee_no = employee_no;
            this.report_type = report_type;
            this.wo_no = wo_no;
            this.run_card_no = run_card_no;
            this.op_no = op_no;
            this.op_seq = op_seq;
            this.workstation_no = workstation_no;
            this.machine_no = machine_no;
            this.shift_no = shift_no;
            this.labor_hours = labor_hours;
            this.machine_hours = machine_hours;
            this.reports_qty = reports_qty;
            this.scrap_qty = scrap_qty;
            this.item_no = item_no;
            this.site_no = site_no;
            this.abnormal_no = abnormal_no;
            this.defect_qty = defect_qty;
            this.seq = seq;
            this.component_item_no = component_item_no;
            this.qty = qty;
            //20170406 add by wangyq for P001-170327001 ==========begin==============
            this.lot_no = lot_no;
            this.warehouse_no = warehouse_no;
            this.storage_spaces_no = storage_spaces_no;
            //20170406 add by wangyq for P001-170327001 ==========end==============
        }

        public ParaObject() { }
        #endregion
    }
}
