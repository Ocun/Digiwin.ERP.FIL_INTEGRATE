//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>liwei1</author>
//<createDate>2017-04-12</createDate>
//<description>料件条码创建服务</description>
//---------------------------------------------------------------- 
//20170727 modi by shenbao for P001-170717001。
//20170901 modi by liwei1 for P001-170717001
//20170918 modi by liwei1 for B001-170918003

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.BC_REG.Business;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement{

    [ServiceClass(typeof(IInsertItemBcRecodeService))]
    [Description("料件条码创建服务")]
    public class InsertItemBcRecodeService:ServiceComponent,IInsertItemBcRecodeService{

        /// <summary>
        /// 记录生成的批号
        /// </summary>
        private Dictionary<string, string> _lotCodeDictionary;//20170918 add by liwei1 for B001-170918003

        /// <summary>
        /// 记录生成的批条码(批条码不需要生成重复的)
        /// </summary>
        private List<string> _bcRecord;//20170918 add by liwei1 for B001-170918003

        /// <summary>
        /// 根据传入的送货单单号信息获取根据明细产生条码基础资料档
        /// </summary>
        /// <param name="delivery_no">送货单号</param>
        /// <returns></returns>
        public void InsertItemBcRecode(string delivery_no){
            try{
                //参数检查
                IInfoEncodeContainer infoEnCode = GetService<IInfoEncodeContainer>();
                if(Maths.IsEmpty(delivery_no)){
                    throw new BusinessRuleException(infoEnCode.GetMessage("A111201","delivery_no")); //‘入参【delivery_no】未传值’( A111201)
                }
                using(ITransactionService transActionService = GetService<ITransactionService>()){
                    //20170901 add by liwei1 for P001-170717001
                    //查询品号条码规则信息
                    DependencyObjectCollection bcReg = BcRegData(delivery_no);

                    //初始化
                    _lotCodeDictionary = new Dictionary<string, string>();//20170918 add by liwei1 for B001-170918003
                    _bcRecord = new List<string>();//20170918 add by liwei1 for B001-170918003

                    //生成主键服务 
                    IPrimaryKeyService keyService = GetServiceForThisTypeKey<IPrimaryKeyService>();
                    ILogOnService loginSrv = GetService<ILogOnService>();
                    IQueryBarCodeRulePropertyService queryBarCodeRuleSer = GetService<IQueryBarCodeRulePropertyService>("BC_REG");

                    DataTable bcRecord = CreateBcRecordInfo(); //存储条码档预生成数据
                    DataTable bcRecordD = CreateBcRecordDInfo(); //存储条码档单身预生成数据
                    DataTable itemLotDt = CreateItemLotInfo(); //存储品号批号预生成数据 //20170901 add by liwei1 for P001-170717001
                    //object bcRegId = Maths.GuidDefaultValue(); //记录上一步条码规则id  //20170901 mark by liwei1 for P001-170717001
                    foreach(DependencyObject itemBcReg in bcReg){
                        #region //20170901 mark by liwei1 for P001-170717001 默认值会更新，无需这样处理

                        ////因为是利用条码编码规则默认值生成条码，如果【FIL品号条码规格信息】存在相同的会生成相同的条码，根据和SD确认，如果BC_REG_ID相同的第二笔相同的条码规则id（或者BC_REG_ID为空）不生成条码，
                        //if (bcRegId.Equals(itemBcReg["BC_REG_ID"]) ||
                        //    itemBcReg["BC_REG_ID"].Equals(Maths.GuidDefaultValue())){
                        //    continue;
                        //}
                        //else{
                        //    bcRegId = itemBcReg["BC_REG_ID"]; //记录可以生成条码对应BC_REG_ID
                        //}

                        #endregion

                        //⑴	调用“查询条码编码规则属性项服务”得到此条码规则的属性项清单
                        object[] result = queryBarCodeRuleSer.QueryBarCodeRuleProperty(itemBcReg["BC_REG_ID"]);
                        if(result.Length <= 0) continue;
                        //条码编码规则属性项信息
                        DependencyObjectCollection barCodeRuleProInfo = result[0] as DependencyObjectCollection;
                        //条码编码规则替换明细
                        DependencyObjectCollection barCodeRuleProSDInfo = result[1] as DependencyObjectCollection;

                        //（2）查询条码规则对应的属性项及相关设置   将返回的条码规则的属性项清单排序 (无需重新排序，服务返回的结果已经按照要求排序了)
                        //条码编号
                        GenerateBcCode(barCodeRuleProInfo,barCodeRuleProSDInfo,itemBcReg["ITEM_ID"],itemBcReg["ITEM_FEATURE_ID"],infoEnCode,keyService,loginSrv,itemBcReg,bcRecord,bcRecordD,
                            itemLotDt//20170901 add by liwei1 for P001-170717001
                            );
                    }
                    //存在需要插入的数据
                    if(bcRecord.Rows.Count > 0){
                        //using (ITransactionService transActionService = this.GetService<ITransactionService>()) {//20170901 mark by liwei1 for P001-170717001 事务放在最外围，期间可能存在最大流水码更新操作，那么能自动回滚

                        #region //20170901 add by liwei1 for P001-170717001 //插入品号条码数据

                        //插入品号条码数据
                        if(itemLotDt.Rows.Count > 0){
                            List<BulkCopyColumnMapping> mappingItemLot = GetBulkCopyColumnMapping(itemLotDt.Columns);
                            GetService<IQueryService>().BulkCopy(itemLotDt,itemLotDt.TableName,mappingItemLot.ToArray());
                        }

                        #endregion

                        //插入条码档数据
                        List<BulkCopyColumnMapping> mapping = GetBulkCopyColumnMapping(bcRecord.Columns);
                        GetService<IQueryService>().BulkCopy(bcRecord,bcRecord.TableName,mapping.ToArray());

                        //插入条码档单身数据
                        List<BulkCopyColumnMapping> mappingD = GetBulkCopyColumnMapping(bcRecordD.Columns);
                        GetService<IQueryService>().BulkCopy(bcRecordD,bcRecordD.TableName,mappingD.ToArray());

                        transActionService.Complete();
                    }
                }
            } catch(Exception){
                throw;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="paraBcRegDCol"></param>
        /// <param name="paraBcRegSDCol"></param>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <param name="infoEnCode"></param>
        /// <param name="loginSrv"></param>
        /// <param name="itemBcReg"></param>
        /// <param name="keyService"></param>
        /// <param name="bcRecord"></param>
        /// <param name="bcRecordD"></param>
        /// <param name="itemLotDt"></param>
        /// <returns></returns>
        private void GenerateBcCode(DependencyObjectCollection paraBcRegDCol,DependencyObjectCollection paraBcRegSDCol,object itemId,object itemFeatureId,IInfoEncodeContainer infoEnCode,
            IPrimaryKeyService keyService,ILogOnService loginSrv,DependencyObject itemBcReg,DataTable bcRecord
			,DataTable bcRecordD,DataTable itemLotDt //20170901 add by liwei1 for P001-170717001
            ){
            #region //20170901 add by liwei1 for P001-170717001 注释掉原有局部变量及部分逻辑

            //int includedCodingCount = 0;
            //bool propertyValueIsOk = true; //PropertyValue [默认值]不为空
            //object bcPropertySysId = Maths.GuidDefaultValue(); //流水号属性ID
            //int paraNum = 1; //默认条码个数
            //int remainder = 0; //箱数的余数
            //int paraBcBegin = 0; //起始编码
            //int paraBcEnd = 0; //截止流水号

            ////删除集合中满足条件的数据，必须使用for循环
            //for (int i = 0; i < paraBcRegDCol.Count; i++) {
            //    DependencyObject item = paraBcRegDCol[i];
            //    //IncludedCoding[纳入编码]=TRUE，记录对应数据
            //    if (item["IncludedCoding"].ToBoolean()) {
            //        includedCodingCount++;
            //    } else {
            //        //移除不纳入编码的记录
            //        paraBcRegDCol.Remove(paraBcRegDCol[i]);
            //        i--;
            //        continue;
            //    }

            //    //默认值为空，且不是流水号
            //    if (Maths.IsEmpty(item["PropertyValue"]) && !item["BC_PROPERTY_CODE"].Equals("Sys_Sn")) {
            //        propertyValueIsOk = false;
            //    }
            //    //判断查询出的条码编码规则对应的条码属性项中是否有流水号属性
            //    if (item["BC_PROPERTY_CODE"].ToStringExtension() == "Sys_Sn") {
            //        bcPropertySysId = item["Property"];
            //        paraBcBegin = item["PropertyValue"].ToInt32();
            //    }
            //}

            //if (includedCodingCount <= 0) {
            //    throw new BusinessRuleException(infoEnCode.GetMessage("A111416"));//不存在纳入编码的属性值，请确认！
            //}

            ////记录的PropertyValue [默认值]不为空执行下面生成条码服务
            //if (propertyValueIsOk) {
            ////计算可生成条码个数
            //if (paraBcBegin != 0 && paraBcEnd != 0){
            //    paraNum = paraBcBegin - paraBcEnd + 1;
            //}

            #endregion

            #region //20170901 add by liwei1 for P001-170717001 计算生成条码个数/获取最大起始编码

            int paraNum = 1; //默认条码个数
            decimal remainder = 0; //箱数的余数
            int paraBcBegin = 0; //起始编码
            string ruleDetailPre = string.Empty; //前缀
            string ruleDetailSuf = string.Empty; //后缀
            bool isSysSn = false; //是否存在流水码
            string propertyValueSysSn = string.Empty;
            Common.Business.ILotNoAndSerialNoCodingService lotNoAndSerialNoSrv = GetService<Common.Business.ILotNoAndSerialNoCodingService>(TypeKey);

            foreach(DependencyObject item in paraBcRegDCol){
                //属性项（BC_PROPERTY_CODE）中存在流水号（‘Sys_Sn’）
                if(item["BC_PROPERTY_CODE"].ToStringExtension() == "Sys_Sn"){
                    //生成条码的个数
                    /*若实发量(Query.ACTUAL_QTY)除去箱装量（Query.PACKING_QTY）余数=0，
                     * 则个数 =实发量(Query.ACTUAL_QTY)/ 箱装量（Query.PACKING_QTY），
                     * 否则个数 =实发量(Query.ACTUAL_QTY)/ 箱装量（Query.PACKING_QTY）+1 
                     * 注意：箱装量为0的处理
                     */
                    //余数
                    remainder = (itemBcReg["ACTUAL_QTY"].ToDecimal() % itemBcReg["PACKING_QTY"].ToDecimal());
                    decimal num = itemBcReg["ACTUAL_QTY"].ToDecimal() / itemBcReg["PACKING_QTY"].ToDecimal();
                    paraNum = remainder == 0m ? num.ToInt32() : Math.Ceiling(num).ToInt32();
                    //存在流水码
                    isSysSn = true;
                }
            }

            //更新默认值：PropertyValue [默认值]
            UpdatePropertyValue(keyService,loginSrv,itemBcReg,itemLotDt,paraBcRegDCol,lotNoAndSerialNoSrv);

            //批条码如果存在重复，无需做条码拼接，直接返回。主方法循环拼接下一个条码
            if (itemBcReg["BARCODE_TYPE"].ToStringExtension() == "1") {
                string bcRecordStr = paraBcRegDCol.Aggregate(string.Empty,(current,item) => current + item["PropertyValue"].ToStringExtension());
                if(!_bcRecord.Contains(bcRecordStr)){
                    _bcRecord.Add(bcRecordStr);
                } else{
                    return;
                }
            }

            if(isSysSn){
                //更新前缀\后缀
                SetSnpSfix(paraBcRegDCol,paraBcRegSDCol,out ruleDetailPre,out ruleDetailSuf);
                //最大流水号
                object maxSn = GetMaxSn(itemBcReg["BC_REG_ID"],ruleDetailPre,ruleDetailSuf);
                //起始编号流水号
                paraBcBegin = maxSn.ToInt32() + 1;
            }

            #endregion

            for(int j = 0;j < paraNum;j++){
                string barCodeNo = string.Empty; //条码初始化
                //主键
                object bcRecordId = keyService.CreateId();

                #region 生成新条码并生成条码档单身数据

                int num = 1; //单身序号初始值
                foreach(DependencyObject item in paraBcRegDCol){

                    #region //20170901 add by liwei1 for P001-170717001 更新库存单位数量

                    if(item["BC_PROPERTY_CODE"].ToStringExtension() == "Sys_INV_Qty"){
                        //库存单位数量
                        if(itemBcReg["BARCODE_TYPE"].ToStringExtension() == "1"){
                            item["PropertyValue"] = itemBcReg["PACKING_QTY"].ToDecimal().ToString("#0.########");
                        } else if(itemBcReg["BARCODE_TYPE"].ToStringExtension() == "2"){
                            //最后一箱，并且最后一箱没有装满 (库存单位数量取位零，不然编码长度过长)
                            if(paraNum == j + 1 && remainder != 0){
                                item["PropertyValue"] = remainder.ToString("#0.########");
                            } else{
                                item["PropertyValue"] = itemBcReg["PACKING_QTY"].ToDecimal().ToString("#0.########");
                            }
                        }
                    }

                    //纳入编码属性项才做条码拼接
                    if(item["IncludedCoding"].ToBoolean()){
                        #endregion

                        #region //20170901 mark by liwei1 for P001-170717001 生成条码的时候批号是直接自动生成的并直接生效，无需做校验

                        //if (item["BC_PROPERTY_CODE"].ToStringExtension() == "Sys_Item_Lot"){
                        //    if (!ValidateItemLot(itemId, itemFeatureId, item["PropertyValue"].ToStringExtension())){
                        //        throw new BusinessRuleException(infoEnCode.GetMessage("A111417")); //您输入的批号不存在或未生效！
                        //    }
                        //}

                        #endregion

                        string subBarCodeNo = string.Empty; //当前记录生成的条码
                        int propertySize = item["PropertyValue"].ToStringExtension().Length; //属性值字符串长度
                        int substringSize = item["SubStringSize"].ToInt32(); //取位长度 
                        int codingSize = item["CodingSize"].ToInt32(); //编码长度

                        if(item["BC_PROPERTY_CODE"].ToStringExtension() == "Sys_Sn"){
                            //如果当前属性值为流水号      按照取位长度（SUBSTRINGSIZE）不足位左边补0
                            item["PropertyValue"] = (paraBcBegin + j).ToStringExtension().PadLeft(codingSize,'0');
                            propertyValueSysSn = item["PropertyValue"].ToStringExtension();
                        }

                        //（1）取位   
                        switch(item["SubStringType"].ToStringExtension()){
                            case "0": //取位方式为'0.全部'
                                subBarCodeNo = item["PropertyValue"].ToStringExtension();
                                if(codingSize > propertySize)
                                    for(int i = 0;i < (codingSize - propertySize);i++){
                                        subBarCodeNo += item["CoverDefault"].ToStringExtension();
                                    }
                                break;
                            case "1": //取位方式为'1.左'
                                if(substringSize > propertySize){
                                    string coverDefaultTemp = string.Empty;
                                    //拼接不足位字符串
                                    for(int i = 0;i < substringSize - propertySize;i++){
                                        coverDefaultTemp += item["CoverDefault"].ToStringExtension();
                                    }
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension() + coverDefaultTemp;
                                } else{
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension().Substring(0,substringSize);
                                }
                                break;
                            case "2": //取位方式为'2.右'
                                if(substringSize > propertySize){
                                    string coverDefaultTemp = string.Empty;
                                    //拼接不足位字符串
                                    for(int i = 0;i < substringSize - propertySize;i++){
                                        coverDefaultTemp += item["CoverDefault"].ToStringExtension();
                                    }
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension() + coverDefaultTemp;
                                } else{
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension().Substring(propertySize - substringSize,substringSize);
                                }
                                break;
                            case "3": //取位方式为'3.区间'
                                if(item["SubStringStart"].ToInt32() > propertySize){
                                    throw new BusinessRuleException(infoEnCode.GetMessage("A111418")); //起始位不能大于属性值长度! 
                                }
                                if((propertySize - item["SubStringStart"].ToInt32()) < substringSize){
                                    string coverDefaultTemp = string.Empty;
                                    //拼接不足位字符串
                                    for(int i = 0;i < substringSize - (propertySize - item["SubStringStart"].ToDecimal() + 1);i++){
                                        coverDefaultTemp += item["CoverDefault"].ToStringExtension();
                                    }
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension().Substring(item["SubStringStart"].ToInt32() - 1) + coverDefaultTemp;
                                } else{
                                    subBarCodeNo = item["PropertyValue"].ToStringExtension().Substring(item["SubStringStart"].ToInt32() - 1,substringSize);
                                }
                                break;
                        }

                        //（2）替换
                        if((paraBcRegSDCol.Count > 0)){
                            foreach(DependencyObject detail in paraBcRegSDCol){
                                if(subBarCodeNo.Equals(detail["ORIGINAL_VALUE"])){
                                    subBarCodeNo = detail["REPLACED_VALUE"].ToStringExtension();
                                    break;
                                }
                            }
                        }
                        //（3）进制
                        int barCodeNoToInt;
                        if((item["DataType"].Equals("2")) && (item["Notation"].Equals("2")) && (Int32.TryParse(subBarCodeNo,out barCodeNoToInt))){
                            subBarCodeNo = barCodeNoToInt.ToString("X");
                        }
                        if(subBarCodeNo.Length != codingSize){
                            throw new BusinessRuleException(infoEnCode.GetMessage("A111419")); //生成的条码不符合编码长度!
                        }
                        barCodeNo += subBarCodeNo; //将已生成的条码与这次循环的条码拼接
                    } //20170901 add by liwei1 for P001-170717001
                    //增加条码档单身记录行
                    DataRow dr = bcRecordD.NewRow();
                    dr["BC_RECORD_D_ID"] = keyService.CreateId(); //主键
                    dr["ParentId"] = bcRecordId; //父主键
                    dr["SequenceNumber"] = num; //序号
                    dr["BC_PROPERTY_ID"] = item["Property"]; //属性项
                    dr["BC_PROPERTY_VALUE"] = item["PropertyValue"]; //属性值
                    dr["REMARK"] = string.Empty; //备注
                    dr["BARCODE_VALUE"] = string.Empty; //条码值
                    dr["CreateBy"] = loginSrv.CurrentUserId;
                    dr["CreateDate"] = DateTime.Now;
                    dr["ModifiedBy"] = loginSrv.CurrentUserId;
                    dr["ModifiedDate"] = DateTime.Now;
                    dr["LastModifiedBy"] = loginSrv.CurrentUserId;
                    dr["LastModifiedDate"] = DateTime.Now;
                    bcRecordD.Rows.Add(dr);

                    num++; //序号自增
                }

                #endregion

                #region 如果新生成的条码不为空，给条码档增加记录行

                if(!Maths.IsEmpty(barCodeNo)){
                    DataRow dr = bcRecord.NewRow();
                    dr["BC_RECORD_ID"] = bcRecordId;
                    dr["BARCODE_NO"] = barCodeNo;
                    dr["BC_REG_ID"] = itemBcReg["BC_REG_ID"];
                    dr["ITEM_ID"] = itemBcReg["ITEM_ID"];
                    dr["ITEM_FEATURE_ID"] = itemBcReg["ITEM_FEATURE_ID"];
                    dr["GENERATE_TYPE"] = "2";
                    dr["SOURCE_ID_RTK"] = "FIL_ARRIVAL";
                    dr["SOURCE_ID_ROid"] = itemBcReg["FIL_ARRIVAL_ID"]; //20170901 modi by liwei1 for P001-170717001 old:FIL_ARRIVAL_D_ID
                    dr["GENERATE_DATE"] = DateTime.Now.Date; //20170901 modi by liwei1 for P001-170717001 old:DateTime.Now
                    dr["REMARK"] = string.Empty;
                    dr["Owner_Org_RTK"] = "PARA_GROUP";
                    dr["Owner_Org_ROid"] = Maths.GuidDefaultValue();
                    dr["LABEL_LAYOUT"] = string.Empty;
                    dr["SOURCE_D_ID_RTK"] = "PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD"; //20170901 modi by liwei1 for P001-170717001 old:"FIL_ARRIVAL.FIL_ARRIVAL_D"
                    dr["SOURCE_D_ID_ROid"] = itemBcReg["PURCHASE_ORDER_SD_ID"]; //20170901 modi by liwei1 for P001-170717001 old:itemBcReg["FIL_ARRIVAL_D_ID"]
                    dr["BARCODE_TYPE"] = itemBcReg["BARCODE_TYPE"];
                    dr["ApproveStatus"] = "N";
                    dr["CreateBy"] = loginSrv.CurrentUserId;
                    dr["CreateDate"] = DateTime.Now;
                    dr["ModifiedBy"] = loginSrv.CurrentUserId;
                    dr["ModifiedDate"] = DateTime.Now;
                    dr["LastModifiedBy"] = loginSrv.CurrentUserId;
                    dr["LastModifiedDate"] = DateTime.Now;
                    bcRecord.Rows.Add(dr);
                }

                #endregion
            }

            #region//20170901 add by liwei1 for P001-170717001 //存在流水号才更新或者插入

            if(isSysSn){
                //更新最大流水号
                InsertOrUpdateSnItemRecordMaX(keyService,itemBcReg,propertyValueSysSn,paraBcBegin,ruleDetailPre,ruleDetailSuf);
            }

            #endregion

            //}//20170901 add by liwei1 for P001-170717001

            //return barCodeNo;
        }

        #region //20170901 add by liwei1 for P001-170717001

        /// <summary>
        /// 替换默认属性值
        /// </summary>
        /// <param name="item"></param>
        /// <param name="paraBcRegSDCol"></param>replace
        /// <returns></returns>
        private string ReplacePropertyValue(DependencyObject item,DependencyObjectCollection paraBcRegSDCol){
            string subBarCodeNo = item["PropertyValue"].ToStringExtension();
            if((paraBcRegSDCol.Count > 0)){
                //（2）替换
                foreach(DependencyObject detail in paraBcRegSDCol){
                    if(item["BC_REG_D_ID"].Equals(detail["BC_REG_D_ID"]) && subBarCodeNo.Equals(detail["ORIGINAL_VALUE"])){
                        subBarCodeNo = detail["REPLACED_VALUE"].ToStringExtension();
                        break;
                    }
                }
            }
            return subBarCodeNo;
        }


        /// <summary>
        /// 更新最大流水号
        /// </summary>
        /// <param name="keyService"></param>
        /// <param name="itemBcReg"></param>
        /// <param name="propertyValueSysSn"></param>
        /// <param name="paraBcBegin"></param>
        /// <param name="ruleDetailPre"></param>
        /// <param name="ruleDetailSuf"></param>
        private void InsertOrUpdateSnItemRecordMaX(IPrimaryKeyService keyService,DependencyObject itemBcReg,string propertyValueSysSn,int paraBcBegin,string ruleDetailPre,string ruleDetailSuf){
            //最大流水码不为空
            if(!Maths.IsEmpty(propertyValueSysSn)){
                QueryNode node;
                //paraBcBegin为1的时候说明不存在最大流水号
                if(paraBcBegin == 1){
                    //插入只有一笔数据，便不批量处理
                    Dictionary<string,QueryProperty> columns = new Dictionary<string,QueryProperty>();
                    columns.Add("SN_ITEM_RECORD_MAX_ID",OOQL.CreateConstants(keyService.CreateId()));
                    columns.Add("PREFIX",OOQL.CreateConstants(ruleDetailPre));
                    columns.Add("SUFFIX",OOQL.CreateConstants(ruleDetailSuf));
                    columns.Add("MAX_SN",OOQL.CreateConstants(propertyValueSysSn));
                    columns.Add("BC_REG_ID",OOQL.CreateConstants(itemBcReg["BC_REG_ID"]));
                    node = OOQL.Insert("SN_ITEM_RECORD_MAX",columns.Keys.ToArray()).Values(columns.Values.ToArray());
                } else{
                    node = OOQL.Update("SN_ITEM_RECORD_MAX",new[]{new SetItem(
                                            OOQL.CreateProperty("SN_ITEM_RECORD_MAX.MAX_SN"),
                                            OOQL.CreateConstants(propertyValueSysSn))})
                                        .Where((OOQL.CreateProperty("SN_ITEM_RECORD_MAX.BC_REG_ID") == OOQL.CreateConstants(itemBcReg["BC_REG_ID"]))
                                            & (OOQL.CreateProperty("SN_ITEM_RECORD_MAX.PREFIX") == OOQL.CreateConstants(ruleDetailPre)) 
                                            & (OOQL.CreateProperty("SN_ITEM_RECORD_MAX.SUFFIX") == OOQL.CreateConstants(ruleDetailSuf)));
                }
                GetService<IQueryService>().ExecuteNoQueryWithManageProperties(node);
            }
        }

        /// <summary>
        /// 更新条码属性项默认值，用于拼接条码
        /// </summary>
        /// <param name="keyService"></param>
        /// <param name="loginSrv"></param>
        /// <param name="itemBcReg"></param>
        /// <param name="itemLotDt"></param>
        /// <param name="paraBcRegDCol"></param>
        /// <param name="lotNoAndSerialNoSrv"></param>
        private void UpdatePropertyValue(IPrimaryKeyService keyService,ILogOnService loginSrv,DependencyObject itemBcReg,DataTable itemLotDt,DependencyObjectCollection paraBcRegDCol,
            Common.Business.ILotNoAndSerialNoCodingService lotNoAndSerialNoSrv){
            
            foreach(DependencyObject item in paraBcRegDCol){
                switch(item["BC_PROPERTY_CODE"].ToStringExtension()){
                    case "Sys_ItemCode": //品号
                        item["PropertyValue"] = itemBcReg["ITEM_CODE"];
                        break;
                    case "Sys_ItemFeature": //特征码
                        item["PropertyValue"] = itemBcReg["ITEM_FEATURE_CODE"];
                        break;
                    case "Sys_Item_Lot": //批号
                        //20170918 add by liwei1 for B001-170918003 ===begin===
                        string key = itemBcReg["PLANT_ID"].ToStringExtension() + itemBcReg["ITEM_ID"].ToStringExtension() + itemBcReg["SUPPLIER_ID"].ToStringExtension() + 
                            itemBcReg["ITEM_FEATURE_ID"].ToStringExtension();
                        if (!_lotCodeDictionary.ContainsKey(key)){
                            //20170918 add by liwei1 for B001-170918003 ===end===

                            DependencyObject lotNoAndSerialNoCoding = lotNoAndSerialNoSrv.LotNoAndSerialNoCoding("1",itemBcReg["PLANT_ID"],itemBcReg["ITEM_ID"],
                                itemBcReg["SUPPLIER_ID"],itemBcReg["ITEM_FEATURE_ID"],DateTime.Now,false);
                            item["PropertyValue"] = lotNoAndSerialNoCoding["LOT_CODE"];

                            DataRow itemLotRow = itemLotDt.NewRow();
                            itemLotRow["ITEM_LOT_ID"] = keyService.CreateId(); //主键
                            itemLotRow["ALLOW_ISSUE_DATE"] = lotNoAndSerialNoCoding["ALLOW_ISSUE_DATE"];
                            itemLotRow["EFFECTIVE_DATE"] = lotNoAndSerialNoCoding["EFFECTIVE_DATE"];
                            itemLotRow["FIRST_RECEIPT_DATE"] = OrmDataOption.EmptyDateTime;
                            itemLotRow["LAST_RECEIPT_DATE"] = OrmDataOption.EmptyDateTime;
                            itemLotRow["LAST_ISSUE_DATE"] = OrmDataOption.EmptyDateTime;
                            itemLotRow["LAST_COUNT_DATE"] = OrmDataOption.EmptyDateTime;
                            itemLotRow["REMARK"] = string.Empty;
                            itemLotRow["LOT_CODE"] = lotNoAndSerialNoCoding["LOT_CODE"];
                            itemLotRow["INEFFECTIVE_DATE"] = lotNoAndSerialNoCoding["INEFFECTIVE_DATE"];
                            itemLotRow["LOT_DESCRIPTION"] = lotNoAndSerialNoCoding["LOT_CODE"];
                            itemLotRow["SOURCE_ID_RTK"] = string.Empty;
                            itemLotRow["SOURCE_ID_ROid"] = Maths.GuidDefaultValue();
                            itemLotRow["ITEM_ID"] = itemBcReg["ITEM_ID"];
                            itemLotRow["ITEM_FEATURE_ID"] = itemBcReg["ITEM_FEATURE_ID"];
                            itemLotRow["Owner_Org_RTK"] = "PARA_GROUP";
                            itemLotRow["Owner_Org_ROid"] = Maths.GuidDefaultValue();
                            itemLotRow["Owner_Dept"] = Maths.GuidDefaultValue();
                            itemLotRow["Owner_Emp"] = Maths.GuidDefaultValue();
                            itemLotRow["ApproveStatus"] = "Y";
                            itemLotRow["CreateBy"] = loginSrv.CurrentUserId;
                            itemLotRow["CreateDate"] = DateTime.Now;
                            itemLotRow["ModifiedBy"] = loginSrv.CurrentUserId;
                            itemLotRow["ModifiedDate"] = DateTime.Now;
                            itemLotRow["LastModifiedBy"] = loginSrv.CurrentUserId;
                            itemLotRow["LastModifiedDate"] = DateTime.Now;
                            itemLotDt.Rows.Add(itemLotRow);

                            //20170918 add by liwei1 for B001-170918003 ===begin===
                            _lotCodeDictionary.Add(key, item["PropertyValue"].ToStringExtension());
                        } else{
                            item["PropertyValue"] = _lotCodeDictionary[key];                            
                        }
                        //20170918 add by liwei1 for B001-170918003 ===end===
                        break;
                    case "Sys_Enterprise_Code": //供应商
                        item["PropertyValue"] = itemBcReg["COMPANY_CODE"];
                        break;
                    case "Sys_Year": //年
                        item["PropertyValue"] = DateTime.Now.Year.ToStringExtension();
                        break;
                    case "Sys_Month": //月
                        item["PropertyValue"] = DateTime.Now.Month.ToStringExtension();
                        break;
                    case "Sys_Day": //日
                        item["PropertyValue"] = DateTime.Now.Day.ToStringExtension();
                        break;
                    case "Sys_INV_Qty": //库存单位数量
                        //获取最大流水码的时候默认应该是装箱量（和SA、SD确认暂时不考虑最后一箱余数装箱量不一样，应该重新获取最大流水号的情况）
                        item["PropertyValue"] = itemBcReg["PACKING_QTY"].ToStringExtension();
                        break;
                }
            }
        }

        /// <summary>
        /// 给最大流水号前缀、后缀赋值
        /// </summary>
        /// <param name="entity">条码编码规则属性项信息</param>
        /// <param name="paraBcRegSDCol">替换明细</param>
        /// <param name="ruleDetailPre">前缀</param>
        /// <param name="ruleDetailSuf">后缀</param>
        private void SetSnpSfix(DependencyObjectCollection entity,DependencyObjectCollection paraBcRegSDCol,out string ruleDetailPre,out string ruleDetailSuf){
            var sortedRule = entity.OrderBy(x => x["SequenceNumber"]); //界面显示行按照 SequenceNumber[序号] 升序排序
            bool isPreDetail = true;
            ruleDetailPre = ruleDetailSuf = string.Empty;
            foreach(var item in sortedRule){
                //纳入编码属性项才拼接前缀后缀
                if(!item["IncludedCoding"].ToBoolean()) continue;
                //如果存在可以替换的值便替换PropertyValue
                item["PropertyValue"] = ReplacePropertyValue(item,paraBcRegSDCol);
                //找出  RULE_DETAIL 中 ui BC_PROPERTY_CODE 包含‘Sys_Sn’’ 行的SequenceNumber[序号]赋值给SEQ 
                if((item["BC_PROPERTY_CODE"].ToStringExtension().Contains("Sys_Sn"))){
                    isPreDetail = false; //标志位，当为true时表示当前流水号小于SEQ，否则为大于SEQ
                    continue;
                }
                //isPreDetail为true拼接前缀，否则拼接至后缀
                if(isPreDetail){
                    ruleDetailPre += item["PropertyValue"].ToStringExtension().Trim();
                } else{
                    ruleDetailSuf += item["PropertyValue"].ToStringExtension().Trim();
                }
            }
        }

        /// <summary>
        /// 取最大流水号
        /// </summary>
        /// <param name="bcRegId">条码规则编码</param>
        /// <param name="prefix">前缀</param>
        /// <param name="suffix">后缀</param>
        /// <returns></returns>
        private object GetMaxSn(object bcRegId,string prefix,string suffix){
            QueryNode node =
                OOQL.Select(1,OOQL.CreateProperty("SN_ITEM_RECORD_MAX.MAX_SN"))
                    .From("SN_ITEM_RECORD_MAX")
                    .Where(OOQL.CreateProperty("SN_ITEM_RECORD_MAX.BC_REG_ID") == OOQL.CreateConstants(bcRegId) & OOQL.CreateProperty("SN_ITEM_RECORD_MAX.PREFIX") == OOQL.CreateConstants(prefix) &
                           OOQL.CreateProperty("SN_ITEM_RECORD_MAX.SUFFIX") == OOQL.CreateConstants(suffix));
            return GetService<IQueryService>().ExecuteScalar(node);
        }

        /// <summary>
        /// 创建结构-CreateItemLotInfo[品号批号信息]
        /// </summary>
        /// <returns></returns>
        private DataTable CreateItemLotInfo(){
            DataTable dt = new DataTable();
            dt.TableName = "ITEM_LOT"; //表名
            dt.Columns.Add(new DataColumn("ITEM_LOT_ID",typeof(object))); //	主键
            dt.Columns.Add(new DataColumn("ALLOW_ISSUE_DATE",typeof(DateTime))); //	允许出库日
            dt.Columns.Add(new DataColumn("EFFECTIVE_DATE",typeof(DateTime))); //	生效日期
            dt.Columns.Add(new DataColumn("FIRST_RECEIPT_DATE",typeof(DateTime))); //	首次入库日
            dt.Columns.Add(new DataColumn("LAST_RECEIPT_DATE",typeof(DateTime))); //	最后入库日
            dt.Columns.Add(new DataColumn("LAST_ISSUE_DATE",typeof(DateTime))); //	最后出库日
            dt.Columns.Add(new DataColumn("LAST_COUNT_DATE",typeof(DateTime))); //	上次盘点日
            dt.Columns.Add(new DataColumn("REMARK",typeof(string))); //	备注
            dt.Columns.Add(new DataColumn("LOT_CODE",typeof(string))); //	批号
            dt.Columns.Add(new DataColumn("INEFFECTIVE_DATE",typeof(DateTime))); //	有效截止日期
            dt.Columns.Add(new DataColumn("LOT_DESCRIPTION",typeof(string))); //	批号说明
            dt.Columns.Add(new DataColumn("SOURCE_ID_RTK",typeof(string))); //	源单ID.RTK
            dt.Columns.Add(new DataColumn("SOURCE_ID_ROid",typeof(object))); //	源单ID.ROid
            dt.Columns.Add(new DataColumn("ITEM_ID",typeof(object))); //	品号
            dt.Columns.Add(new DataColumn("ITEM_FEATURE_ID",typeof(object))); //	特征码
            dt.Columns.Add(new DataColumn("Owner_Org_RTK",typeof(string))); //	来源码
            dt.Columns.Add(new DataColumn("Owner_Org_ROid",typeof(object))); //	源单键值
            dt.Columns.Add(new DataColumn("Owner_Dept",typeof(object))); //	关联部门
            dt.Columns.Add(new DataColumn("Owner_Emp",typeof(object))); //	关联员工
            dt.Columns.Add(new DataColumn("ApproveStatus",typeof(string))); //	状态
            dt.Columns.Add(new DataColumn("CreateBy",typeof(object))); //创建人
            dt.Columns.Add(new DataColumn("CreateDate",typeof(DateTime))); //创建日期
            dt.Columns.Add(new DataColumn("ModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("ModifiedDate",typeof(DateTime))); //
            dt.Columns.Add(new DataColumn("LastModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("LastModifiedDate",typeof(DateTime))); //
            return dt;
        }

        #endregion

        /// <summary>
        ///     通过DataTable列名转换为ColumnMappings
        /// </summary>
        /// <param name="columns">表的列的集合</param>
        /// <returns>Mapping集合</returns>
        private List<BulkCopyColumnMapping> GetBulkCopyColumnMapping(DataColumnCollection columns){
            List<BulkCopyColumnMapping> mapping = new List<BulkCopyColumnMapping>();
            foreach(DataColumn column in columns){
                var targetName = column.ColumnName; //列名
                //列名中的下划线大于0，且以[_RTK]或[_ROid]结尾的列名视为多来源字段
                if((targetName.IndexOf("_",StringComparison.Ordinal) > 0) &&
                   (targetName.EndsWith("_RTK",StringComparison.CurrentCultureIgnoreCase) || targetName.EndsWith("_ROid",StringComparison.CurrentCultureIgnoreCase))){
                    //列名长度
                    var nameLength = targetName.Length;
                    //最后一个下划线后一位位置
                    var endPos = targetName.LastIndexOf("_",StringComparison.Ordinal) + 1;
                    //拼接目标字段名
                    targetName = targetName.Substring(0,endPos - 1) + "." + targetName.Substring(endPos,nameLength - endPos);
                }
                BulkCopyColumnMapping mappingItem = new BulkCopyColumnMapping(column.ColumnName,targetName);
                mapping.Add(mappingItem);
            }
            return mapping;
        }

        /// <summary>
        /// 创建结构-CreateBcRecordInfo[条码档]
        /// </summary>
        /// <returns></returns>
        private DataTable CreateBcRecordInfo(){
            DataTable dt = new DataTable();
            dt.TableName = "BC_RECORD"; //表名
            dt.Columns.Add(new DataColumn("BC_RECORD_ID",typeof(object))); //主键
            dt.Columns.Add(new DataColumn("BARCODE_NO",typeof(string))); //条码编号
            dt.Columns.Add(new DataColumn("BC_REG_ID",typeof(object))); //条码编码规则
            dt.Columns.Add(new DataColumn("ITEM_ID",typeof(object))); //品号
            dt.Columns.Add(new DataColumn("ITEM_FEATURE_ID",typeof(object))); //特征码
            dt.Columns.Add(new DataColumn("GENERATE_TYPE",typeof(string))); //生成方式
            dt.Columns.Add(new DataColumn("SOURCE_ID_RTK",typeof(string))); //来源RTK
            dt.Columns.Add(new DataColumn("SOURCE_ID_ROid",typeof(object))); //来源ROid
            dt.Columns.Add(new DataColumn("GENERATE_DATE",typeof(DateTime))); //生成日期
            dt.Columns.Add(new DataColumn("REMARK",typeof(string))); //备注
            dt.Columns.Add(new DataColumn("Owner_Org_RTK",typeof(string))); //组织RTK
            dt.Columns.Add(new DataColumn("Owner_Org_ROid",typeof(object))); //组织ROid
            dt.Columns.Add(new DataColumn("LABEL_LAYOUT",typeof(string))); //标签样式
            dt.Columns.Add(new DataColumn("SOURCE_D_ID_RTK",typeof(string))); //来源单据单身RTK
            dt.Columns.Add(new DataColumn("SOURCE_D_ID_ROid",typeof(object))); //来源单据单身ROid
            dt.Columns.Add(new DataColumn("BARCODE_TYPE",typeof(string))); //条码类型
            dt.Columns.Add(new DataColumn("ApproveStatus",typeof(string))); //状态码
            dt.Columns.Add(new DataColumn("CreateBy",typeof(object))); //创建人
            dt.Columns.Add(new DataColumn("CreateDate",typeof(DateTime))); //创建日期
            dt.Columns.Add(new DataColumn("ModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("ModifiedDate",typeof(DateTime))); //
            dt.Columns.Add(new DataColumn("LastModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("LastModifiedDate",typeof(DateTime))); //
            return dt;
        }

        /// <summary>
        /// 创建结构-CreateBcRecordDInfo[条码档单身]
        /// </summary>
        /// <returns></returns>
        private DataTable CreateBcRecordDInfo(){
            DataTable dt = new DataTable();
            dt.TableName = "BC_RECORD.BC_RECORD_D"; //表名
            dt.Columns.Add(new DataColumn("BC_RECORD_D_ID",typeof(object))); //主键
            dt.Columns.Add(new DataColumn("ParentId",typeof(object))); //父主键
            dt.Columns.Add(new DataColumn("SequenceNumber",typeof(int))); //序号
            dt.Columns.Add(new DataColumn("BC_PROPERTY_ID",typeof(object))); //属性项
            dt.Columns.Add(new DataColumn("BC_PROPERTY_VALUE",typeof(string))); //属性值
            dt.Columns.Add(new DataColumn("REMARK",typeof(string))); //备注
            dt.Columns.Add(new DataColumn("BARCODE_VALUE",typeof(string))); //条码值
            dt.Columns.Add(new DataColumn("CreateBy",typeof(object))); //创建人
            dt.Columns.Add(new DataColumn("CreateDate",typeof(DateTime))); //创建日期
            dt.Columns.Add(new DataColumn("ModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("ModifiedDate",typeof(DateTime))); //
            dt.Columns.Add(new DataColumn("LastModifiedBy",typeof(object))); //
            dt.Columns.Add(new DataColumn("LastModifiedDate",typeof(DateTime))); //
            return dt;
        }

        #region 数据库相关操作

        /// <summary>
        /// 查询批号是否生效
        /// </summary>
        /// <param name="itemId">品号</param>
        /// <param name="itemFeatureId">特征码</param>
        /// <param name="bcPropertyCode">批号</param>
        /// <returns></returns>
        private bool ValidateItemLot(object itemId,object itemFeatureId,string bcPropertyCode){
            bool isOk = true;
            QueryNode itemLotQuery =
                OOQL.Select(OOQL.CreateProperty("ITEM_LOT.ApproveStatus"))
                    .From("ITEM_LOT","ITEM_LOT")
                    .Where(((OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(bcPropertyCode)) 
							& (OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateConstants(itemId)) 
							& (OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(itemFeatureId)) 
							& OOQL.Exists(
                                OOQL.Select(OOQL.CreateProperty("LOT_CONTROL"))
                                    .From("ITEM","ITEM")
                                    .Where((OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateConstants(itemId)) 
										& (OOQL.CreateProperty("ITEM.LOT_CONTROL") != OOQL.CreateConstants("N"))))));
            if(GetService<IQueryService>().ExecuteScalar(itemLotQuery).ToStringExtension() != "Y"){
                isOk = false;
            }
            return isOk;
        }

        /// <summary>
        /// 查询品号条码规则信息
        /// </summary>
        /// <param name="deliveryNo">送货单号</param>
        /// <returns>集合[条码规格：BC_REG_ID,品号：ITEM_ID,特征码：FIL_ARRIVAL_D]</returns>
        private DependencyObjectCollection BcRegData(string deliveryNo){
            //20170727 add by shenbao for P001-170717001 ===begin===
            QueryNode node = null;
            bool notExistsProperty = CheckExistsProperty();
            if(notExistsProperty){
                //20170727 add by shenbao for P001-170717001 ===end===
                node = OOQL.Select(Formulas.IsNull(OOQL.CreateProperty("FIL_ITEM_BC_REG.BC_REG_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"BC_REG_ID"),OOQL.CreateProperty("ITEM.ITEM_ID"),
                    //20170901 add by liwei1 for P001-170717001 ---start---
                    OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID"),OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE"),OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("FIL_ARRIVAL.COMPANY_CODE"),OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID"),OOQL.CreateProperty("PLANT.PLANT_ID"),OOQL.CreateProperty("FIL_ARRIVAL_D.PACKING_QTY"),
                    OOQL.CreateProperty("FIL_ARRIVAL_D.ACTUAL_QTY"),
                    Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"PURCHASE_ORDER_SD_ID"),
                    //20170901 add by liwei1 for P001-170717001 ---end---
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_FEATURE_ID"),
                    OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID"),OOQL.CreateProperty("BC_REG.BARCODE_TYPE"))
                    .From("FIL_ARRIVAL.FIL_ARRIVAL_D","FIL_ARRIVAL_D")
                    .LeftJoin("ITEM","ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.ITEM_CODE")))
                    .LeftJoin("ITEM.ITEM_FEATURE","ITEM_FEATURE")
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE")) &
                        (OOQL.CreateProperty("ITEM_FEATURE.ITEM_BUSINESS_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")))
                    //20170901 add by liwei1 for P001-170717001 ---start---
                    .LeftJoin("PURCHASE_ORDER","PURCHASE_ORDER")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D","PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")) &
                        (OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE")))
                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD","PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")) &
                        (OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE")))
                    .InnerJoin("FIL_ARRIVAL","FIL_ARRIVAL")
                    .On(OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID"))
                    .InnerJoin("SUPPLIER","SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.COMPANY_CODE"))
                    .InnerJoin("PLANT","PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.PLANT_CODE"))
                    //20170901 add by liwei1 for P001-170717001 ---end---
                    .LeftJoin("FIL_ITEM_BC_REG","FIL_ITEM_BC_REG")
                    .On((OOQL.CreateProperty("FIL_ITEM_BC_REG.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")) &
                        ((OOQL.CreateProperty("FIL_ITEM_BC_REG.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID")) |
                         (OOQL.CreateProperty("FIL_ITEM_BC_REG.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))) &
                        (OOQL.CreateProperty("FIL_ITEM_BC_REG.MAIN") == OOQL.CreateConstants(1,GeneralDBType.Boolean)))
                    .InnerJoin("BC_REG","BC_REG")
                    .On((OOQL.CreateProperty("BC_REG.BC_REG_ID") == OOQL.CreateProperty("FIL_ITEM_BC_REG.BC_REG_ID")))
                    .Where((OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo)))
                    .OrderBy(OOQL.CreateOrderByItem(OOQL.CreateProperty("FIL_ITEM_BC_REG.BC_REG_ID"),SortType.Asc)); //加排序是为了方便后面内存中移除存在相同条码规则的数据
            } else{
                //20170727 add by shenbao for P001-170717001 ===begin===
                node = OOQL.Select(
                    Formulas.Case(null,OOQL.CreateConstants(Maths.GuidDefaultValue()),
                        new CaseItem[]{
                            new CaseItem(OOQL.CreateProperty("BC_REGA.BC_REG_ID").IsNotNull() & OOQL.CreateProperty("BC_REGA.BC_REG_ID") != OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                OOQL.CreateProperty("BC_REGA.BC_REG_ID")),
                            new CaseItem(OOQL.CreateProperty("BC_REGB.BC_REG_ID").IsNotNull() & OOQL.CreateProperty("BC_REGB.BC_REG_ID") != OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                OOQL.CreateProperty("BC_REGB.BC_REG_ID"))
                        },"BC_REG_ID"),OOQL.CreateProperty("ITEM.ITEM_ID"), //20170901 add by liwei1 for P001-170717001 ---start---
                    OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID"),OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_CODE"),OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE"),
                    OOQL.CreateProperty("FIL_ARRIVAL.COMPANY_CODE"),OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID"),OOQL.CreateProperty("PLANT.PLANT_ID"),OOQL.CreateProperty("FIL_ARRIVAL_D.PACKING_QTY"),
                    OOQL.CreateProperty("FIL_ARRIVAL_D.ACTUAL_QTY"),
                    Formulas.IsNull(OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_SD_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"PURCHASE_ORDER_SD_ID"),
                    //20170901 add by liwei1 for P001-170717001 ---end---
                    Formulas.IsNull(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID"),OOQL.CreateConstants(Maths.GuidDefaultValue()),"ITEM_FEATURE_ID"),
                    OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_D_ID"),
                    Formulas.Case(null,OOQL.CreateConstants(""),
                        new CaseItem[]{
                            new CaseItem(OOQL.CreateProperty("BC_REGA.BC_REG_ID").IsNotNull() & OOQL.CreateProperty("BC_REGA.BC_REG_ID") != OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                OOQL.CreateProperty("BC_REGA.BARCODE_TYPE")),
                            new CaseItem(OOQL.CreateProperty("BC_REGB.BC_REG_ID").IsNotNull() & OOQL.CreateProperty("BC_REGB.BC_REG_ID") != OOQL.CreateConstants(Maths.GuidDefaultValue()),
                                OOQL.CreateProperty("BC_REGB.BARCODE_TYPE"))
                        },"BARCODE_TYPE"))
                    .From("FIL_ARRIVAL.FIL_ARRIVAL_D","FIL_ARRIVAL_D")
                    .LeftJoin("ITEM","ITEM")
                    .On((OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_D.ITEM_CODE")))
                    .LeftJoin("ITEM.ITEM_FEATURE","ITEM_FEATURE")
                    .On((OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateProperty("FIL_ARRIVAL_D.ITEM_FEATURE_CODE")) &
                        (OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")))
                    //20170901 add by liwei1 for P001-170717001 ---start---
                    .LeftJoin("PURCHASE_ORDER","PURCHASE_ORDER")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER.DOC_NO") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_NO")))
                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D","PURCHASE_ORDER_D")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_ID") == OOQL.CreateProperty("PURCHASE_ORDER.PURCHASE_ORDER_ID")) &
                        (OOQL.CreateProperty("PURCHASE_ORDER_D.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE")))
                    .LeftJoin("PURCHASE_ORDER.PURCHASE_ORDER_D.PURCHASE_ORDER_SD","PURCHASE_ORDER_SD")
                    .On((OOQL.CreateProperty("PURCHASE_ORDER_SD.PURCHASE_ORDER_D_ID") == OOQL.CreateProperty("PURCHASE_ORDER_D.PURCHASE_ORDER_D_ID")) &
                        (OOQL.CreateProperty("PURCHASE_ORDER_SD.SequenceNumber") == OOQL.CreateProperty("FIL_ARRIVAL_D.ORDER_SE_SE")))
                    .InnerJoin("FIL_ARRIVAL","FIL_ARRIVAL")
                    .On(OOQL.CreateProperty("FIL_ARRIVAL.FIL_ARRIVAL_ID") == OOQL.CreateProperty("FIL_ARRIVAL_D.FIL_ARRIVAL_ID"))
                    .InnerJoin("SUPPLIER","SUPPLIER")
                    .On(OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.COMPANY_CODE"))
                    .InnerJoin("PLANT","PLANT")
                    .On(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateProperty("FIL_ARRIVAL.PLANT_CODE"))
                    //20170901 add by liwei1 for P001-170717001 ---end---
                    .LeftJoin("ITEM_BC_REG","ITEM_BC_REGA")
                    .On((OOQL.CreateProperty("ITEM_BC_REGA.ITEM_ID") == OOQL.CreateProperty("ITEM.ITEM_ID")) &
                        ((OOQL.CreateProperty("ITEM_BC_REGA.ITEM_FEATURE_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID")) |
                         (OOQL.CreateProperty("ITEM_BC_REGA.ITEM_FEATURE_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue()))) &
                        (OOQL.CreateProperty("ITEM_BC_REGA.MAIN") == OOQL.CreateConstants(true)))
                    .LeftJoin("BC_REG","BC_REGA")
                    .On((OOQL.CreateProperty("BC_REGA.BC_REG_ID") == OOQL.CreateProperty("ITEM_BC_REGA.BC_REG_ID")))
                    .LeftJoin("ITEM_BC_REG","ITEM_BC_REGB")
                    .On((OOQL.CreateProperty("ITEM.FEATURE_GROUP_ID") == OOQL.CreateProperty("ITEM_BC_REGB.FEATURE_GROUP_ID")) &
                        (OOQL.CreateProperty("ITEM_BC_REGB.ITEM_ID") == OOQL.CreateConstants(Maths.GuidDefaultValue())) & (OOQL.CreateProperty("ITEM_BC_REGB.MAIN") == OOQL.CreateConstants(true)))
                    .LeftJoin("BC_REG","BC_REGB")
                    .On((OOQL.CreateProperty("BC_REGB.BC_REG_ID") == OOQL.CreateProperty("ITEM_BC_REGB.BC_REG_ID")))
                    .Where((OOQL.CreateProperty("FIL_ARRIVAL_D.DOC_NO") == OOQL.CreateConstants(deliveryNo)));
                //20170727 add by shenbao for P001-170717001 ===end===
            }
            return GetService<IQueryService>().ExecuteDependencyObject(node);
        }

        #endregion

        #region 20170727 modi by shenbao for P001-170717001

        /// <summary>
        /// 实体不存在或者数据库中不存在相关字段，则返回true
        /// </summary>
        /// <returns></returns>
        private bool CheckExistsProperty(){
            bool isInEntity = false;
            bool isInDB = false;
            isInEntity = CheckEntityExistsProperty();
            if(isInEntity){
                isInDB = CheckExistsColumns();
            }

            return !isInEntity || !isInDB;
        }

        /// <summary>
        /// 检查实体存在相关属性
        /// </summary>
        /// <returns></returns>
        private bool CheckEntityExistsProperty(){
            IDataEntityTypeContainer container;
            IDataEntityType entityType;
            this.TryGetService<IDataEntityTypeContainer>(null,out container);
            if(container.DataEntityTypes.TryGet("ITEM_BC_REG",out entityType)){
                //实体存在
                ISimpleProperty qty = entityType.SafeGetProperty("QTY") as ISimpleProperty; //数量
                ISimpleProperty lotAuto = entityType.SafeGetProperty("LOT_AUTO") as ISimpleProperty; //自动生成批号
                ISimpleProperty featureGroup = entityType.SafeGetProperty("FEATURE_GROUP_ID") as ISimpleProperty; //品号群组
                if(qty != null && lotAuto != null && featureGroup != null){
                    //实体存在相关字段
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// 检查数据库中是否存在相关字段
        /// </summary>
        /// <returns></returns>
        private bool CheckExistsColumns(){
            List<string> newColumns = new List<string>(){"QTY","LOT_AUTO","FEATURE_GROUP_ID"};
            string colStr = string.Join(",",newColumns.Select(c => "'" + c + "'").ToArray());
            string sql = string.Format("SELECT 1 FROM sys.sysobjects A INNER JOIN sys.syscolumns B on A.[id] = B.[id] where A.[name] = '{0}' AND B.[name] IN ({1})","ITEM_BC_REG",colStr);
            DataTable tempTable = null;
            using(IConnectionService connSrv = this.GetService<IConnectionService>()){
                IDbCommand cmd = connSrv.CreateDbCommand(DatabaseServerOption.Default);
                cmd.CommandText = sql;

                IDataReader reader = cmd.ExecuteReader();
                tempTable = new DataTable();
                tempTable.Locale = CultureInfo.CurrentCulture;
                tempTable.BeginLoadData();
                tempTable.Load(reader);
                tempTable.EndLoadData();
                tempTable.AcceptChanges();
            }

            return (tempTable != null && tempTable.Rows.Count == newColumns.Count); //列数目与数据库中一致
        }

        #endregion
    }
}
