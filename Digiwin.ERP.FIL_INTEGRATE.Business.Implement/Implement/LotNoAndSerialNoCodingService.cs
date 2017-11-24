//---------------------------------------------------------------- 
//<Author>wangyq</Author>
//<CreateDate>2017/1/3 14:55:32</CreateDate>
//<IssueNO>P001-161215001</IssueNO>
//<Description>移动应用自动产生批号服务</Description>
//----------------------------------------------------------------  
//20170106 modi by wangyq for P001-161230002 增加批号的新增数据库逻辑
//20170109 modi by wangyq for P001-170118001 
//20170509 modi by liwei1 for B001-170505004

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Utils;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement {

    /// <summary>
    ///  
    /// </summary>
    [ServiceClass(typeof(ILotNoAndSerialNoCodingService))]
    [Description("")]
    sealed class LotNoAndSerialNoCodingService : ServiceComponent, ILotNoAndSerialNoCodingService {
        /// <summary>
        /// 移动应用自动产生批号服务
        /// </summary>
        /// <param name="program_job_no">作业编号</param>
        /// <param name="status">执行动作</param>
        /// <param name="item_no">料件编号</param>
        /// <param name="item_feature_no">产品特征</param>
        /// <param name="site_no">营运据点</param>
        /// <param name="object_no">供货商编号//作業編號=1.2.3為必要輸入</param>
        /// <param name="action">动作:Q.查询 I.新增</param>
        /// <param name="lot_no">批号:当动作=Q时，为必输</param>
        /// <returns></returns>
        public Hashtable LotNoAndSerialNoCoding(string program_job_no, string status, string item_no, string item_feature_no, string site_no, string object_no, string action, string lot_no) {//20170109 modi by wangyq for P001-170118001 添加后两个参数
            //20170109 add by wangyq for P001-170118001 =============begin==============
            if (action == "Q" && string.IsNullOrEmpty(lot_no)) {
                throw new ArgumentNullException("lot_no");
            }
            //20170109 add by wangyq for P001-170118001 =============end==============

            string[] convertJobNos = new string[] { "1", "2", "3", "9" };
            DependencyObjectType resultType = new DependencyObjectType("lot_detail");
            resultType.RegisterSimpleProperty("lot_no", typeof(string));
            resultType.RegisterSimpleProperty("item_no", typeof(string));
            resultType.RegisterSimpleProperty("item_feature_no", typeof(string));
            //20170109 add by wangyq for P001-170118001 =============begin==============
            resultType.RegisterSimpleProperty("lot_description", typeof(string));
            resultType.RegisterSimpleProperty("effective_date", typeof(string));
            resultType.RegisterSimpleProperty("effective_deadline", typeof(string));
            resultType.RegisterSimpleProperty("remarks", typeof(string));
            //20170109 add by wangyq for P001-170118001 =============end==============        
            DependencyObjectCollection sourceDocDetail = new DependencyObjectCollection(resultType);
            IdEntity idEntity = ConvertToId(item_no, item_feature_no, site_no, object_no);
            if (action == "I") {//新增逻辑20170109 add by wangyq for P001-170118001
                if (convertJobNos.Contains(program_job_no)) {
                    Digiwin.ERP.Common.Business.ILotNoAndSerialNoCodingService commonLotSerialService = this.GetServiceForThisTypeKey<Digiwin.ERP.Common.Business.ILotNoAndSerialNoCodingService>();
                    DependencyObject commonLotResult = commonLotSerialService.LotNoAndSerialNoCoding("1", idEntity.PlantId, idEntity.ItemId, idEntity.SupplierId, idEntity.ItemFeatureId, DateTime.Now.Date, true);//20170410 modi by wangrm 启萌口述新需求 OLD:false->true
                    DependencyObject resultObj = sourceDocDetail.AddNew();
                    resultObj["lot_no"] = commonLotResult["LOT_CODE"];
                    resultObj["item_no"] = item_no;
                    resultObj["item_feature_no"] = item_feature_no;
                    //20170109 add by wangyq for P001-170118001 =============begin==============
                    //resultObj["lot_description"] = commonLotResult["LOT_CODE"];//20170509 modi by liwei1 for B001-170505004
                    resultObj["lot_description"] = string.Empty;//20170509 add by liwei1 for B001-170505004
                    resultObj["effective_date"] = commonLotResult["EFFECTIVE_DATE"].ToDate().ToString("yyyy-MM-dd");
                    resultObj["effective_deadline"] = commonLotResult["INEFFECTIVE_DATE"].ToDate().ToString("yyyy-MM-dd");
                    resultObj["remarks"] = string.Empty;
                    //20170109 add by wangyq for P001-170118001 =============end==============  
                    InsertItemLot(commonLotResult, idEntity.ItemId, idEntity.ItemFeatureId);//20170106 add by wangyq for P001-161230002 增加批号的新增数据库逻辑
                }
            } else if (action == "Q") { //20170109 add by wangyq for P001-170118001 =============begin==============
                QueryNode node = OOQL.Select(OOQL.CreateProperty("ITEM_LOT.LOT_CODE"),
                                            OOQL.CreateProperty("ITEM_LOT.LOT_DESCRIPTION"),
                                            OOQL.CreateProperty("ITEM_LOT.EFFECTIVE_DATE"),
                                            OOQL.CreateProperty("ITEM_LOT.INEFFECTIVE_DATE"),
                                            OOQL.CreateProperty("ITEM_LOT.REMARK"))
                                    .From("ITEM_LOT", "ITEM_LOT")
                                    .Where(OOQL.AuthFilter("ITEM_LOT", "ITEM_LOT") &
                                          (OOQL.CreateProperty("ITEM_LOT.ITEM_ID") == OOQL.CreateConstants(idEntity.ItemId)
                                           & OOQL.CreateProperty("ITEM_LOT.ITEM_FEATURE_ID") == OOQL.CreateConstants(idEntity.ItemFeatureId)
                                           & OOQL.CreateProperty("ITEM_LOT.LOT_CODE") == OOQL.CreateConstants(lot_no)));
                DependencyObjectCollection lotColl = this.GetService<IQueryService>().ExecuteDependencyObject(node);
                foreach (DependencyObject lotObj in lotColl) {
                    DependencyObject resultObj = sourceDocDetail.AddNew();
                    resultObj["lot_no"] = lotObj["LOT_CODE"];
                    resultObj["item_no"] = item_no;
                    resultObj["item_feature_no"] = item_feature_no;
                    resultObj["lot_description"] = lotObj["LOT_DESCRIPTION"];
                    resultObj["effective_date"] = lotObj["EFFECTIVE_DATE"].ToDate().ToString("yyyy-MM-dd");
                    resultObj["effective_deadline"] = lotObj["INEFFECTIVE_DATE"].ToDate().ToString("yyyy-MM-dd");
                    resultObj["remarks"] = lotObj["REMARK"];
                }
            }
            //20170109 add by wangyq for P001-170118001 =============end==============
            //组合返回结果
            Hashtable result = new Hashtable();
            //添加单据下载数据
            result.Add("lot_detail", sourceDocDetail);
            return result;
        }

        private IdEntity ConvertToId(string item_no, string item_feature_no, string site_no, string object_no) {
            //工厂
            QueryNode node = OOQL.Select(OOQL.CreateProperty("PLANT.PLANT_ID", "id"),
                               OOQL.CreateConstants("PLANT", GeneralDBType.String, "belong"))//标记是PLANT的主键
                        .From("PLANT", "PLANT")
                        .Where(OOQL.CreateProperty("PLANT.PLANT_CODE") == OOQL.CreateConstants(site_no, GeneralDBType.String));
            //品号
            QueryNode tempNode = OOQL.Select(OOQL.CreateProperty("ITEM.ITEM_ID", "id"),
                                   OOQL.CreateConstants("ITEM", GeneralDBType.String, "belong"))
                            .From("ITEM", "ITEM")
                            .Where(OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateConstants(item_no, GeneralDBType.String));
            node = ((WhereNode)node).Union(tempNode);
            //特征码
            if (!string.IsNullOrEmpty(item_feature_no)) {
                tempNode = OOQL.Select(OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_ID", "id"),
                                       OOQL.CreateConstants("ITEM_FEATURE", GeneralDBType.String, "belong"))
                               .From("ITEM", "ITEM")
                               .LeftJoin("ITEM.ITEM_FEATURE", "ITEM_FEATURE")
                               .On(OOQL.CreateProperty("ITEM.ITEM_ID") == OOQL.CreateProperty("ITEM_FEATURE.ITEM_ID"))
                               .Where(OOQL.CreateProperty("ITEM.ITEM_CODE") == OOQL.CreateConstants(item_no, GeneralDBType.String)
                                   & OOQL.CreateProperty("ITEM_FEATURE.ITEM_FEATURE_CODE") == OOQL.CreateConstants(item_feature_no, GeneralDBType.String));
                node = ((UnionNode)node).Union(tempNode);
            }
            //供应商
            tempNode = OOQL.Select(OOQL.CreateProperty("SUPPLIER.SUPPLIER_ID", "id"),
                                  OOQL.CreateConstants("SUPPLIER", GeneralDBType.String, "belong"))
                           .From("SUPPLIER", "SUPPLIER")
                           .Where(OOQL.CreateProperty("SUPPLIER.SUPPLIER_CODE") == OOQL.CreateConstants(object_no, GeneralDBType.String));
            node = ((UnionNode)node).Union(tempNode);
            DependencyObjectCollection idColl = this.GetService<IQueryService>().ExecuteDependencyObject(node);
            IdEntity idEntity = new IdEntity();
            foreach (DependencyObject idObj in idColl) {
                switch (idObj["belong"].ToStringExtension()) {
                    case "PLANT":
                        idEntity.PlantId = idObj["id"];
                        break;
                    case "ITEM":
                        idEntity.ItemId = idObj["id"];
                        break;
                    case "ITEM_FEATURE":
                        idEntity.ItemFeatureId = idObj["id"];
                        break;
                    case "SUPPLIER":
                        idEntity.SupplierId = idObj["id"];
                        break;
                    default:
                        break;
                }
            }
            return idEntity;
        }

        //20170106 add by wangyq for P001-161230002 增加批号的新增数据库逻辑  =============begin=============
        /// <summary>
        /// 
        /// </summary>
        /// <param name="itemLot"></param>
        /// <param name="itemId"></param>
        /// <param name="itemFeatureId"></param>
        private void InsertItemLot(DependencyObject itemLot, object itemId, object itemFeatureId) {
            Dictionary<string, QueryProperty> insertList = new Dictionary<string, QueryProperty>();
            insertList.Add("ITEM_LOT_ID", OOQL.CreateConstants(Guid.NewGuid()));
            insertList.Add("LOT_CODE", OOQL.CreateConstants(itemLot["LOT_CODE"]));
            //insertList.Add("LOT_DESCRIPTION", OOQL.CreateConstants(itemLot["LOT_CODE"]));//20170509 mark by liwei1 for B001-170505004
            insertList.Add("LOT_DESCRIPTION", OOQL.CreateConstants(string.Empty));//20170509 add by liwei1 for B001-170505004
            insertList.Add("ALLOW_ISSUE_DATE", OOQL.CreateConstants(itemLot["ALLOW_ISSUE_DATE"]));
            insertList.Add("EFFECTIVE_DATE", OOQL.CreateConstants(itemLot["EFFECTIVE_DATE"]));
            insertList.Add("INEFFECTIVE_DATE", OOQL.CreateConstants(itemLot["INEFFECTIVE_DATE"]));
            insertList.Add("ITEM_ID", OOQL.CreateConstants(itemId));
            insertList.Add("ITEM_FEATURE_ID", OOQL.CreateConstants(itemFeatureId));
            QueryNode node = OOQL.Insert("ITEM_LOT", insertList.Keys.ToArray()).Values(insertList.Values.ToArray());
            this.GetService<IQueryService>().ExecuteNoQueryWithManageProperties(node);
        }
        //20170106 add by wangyq for P001-161230002 增加批号的新增数据库逻辑  =============end=============
    }
    sealed class IdEntity {
        public IdEntity() {
            PlantId = Maths.GuidDefaultValue();
            ItemId = Maths.GuidDefaultValue();
            ItemFeatureId = Maths.GuidDefaultValue();
            SupplierId = Maths.GuidDefaultValue();
        }
        public object PlantId { get; set; }
        public object ItemId { get; set; }
        public object ItemFeatureId { get; set; }
        public object SupplierId { get; set; }
    }
}
