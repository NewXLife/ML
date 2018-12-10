package ml.train

import com.google.gson.JsonObject

object TestPro {
  val fea_ds =
    """{
      |	"st":{
      |		"params":{
      |			"include":"apply_risk_id,apply_risk_created_at,jxl_contact_call_len_then_equal_1_ratio,jxl_contact_c1w_then_0_ratio,farest_created_span_self_stb,jxl_c1m_gre_0_ratio,jxl_c3m_gre_0_ratio,jxl_contact_call_cnt_equal_1_ratio,jxl_ccm_gre_0_ratio,jxl_region_count_region,jxl_phone_diffdate,hit_type_lijihuankuan_d30,baidu_panshi_prea_score,jxl_coc_gre_0_ratio,latest_yuqi_day_self_stb,mean_first_last_created_span_self_stb,last_latest_created_span_self_stb,hit_type_bangdingshoukuanyinhangka_d30,kuaidi_cnt,max_yuqi_day_self_stb,yiyuqi_cnt,baidu_panshi_duotou_identity_score,jxl_avg_morning,first_last_created_span_self_stb,enter_type_querenjiekuan_d30,fbi_score,overdue_days,equipment_app_names_v2 as equipment_app_name,i011,i061,i301,i601",
      |			"condition":"product_period_count=1 and apply_risk_created_at>= '2018-07-20'and apply_risk_created_at <= '2018-08-05'",
      |			"min_date":"from_unixtime(unix_timestamp(date_sub(current_date(),120),'yyyy-MM-dd'),'yyyyMMdd')",
      |			"max_date":"from_unixtime(unix_timestamp(date_sub(current_date(),0),'yyyy-MM-dd'),'yyyyMMdd')"
      |		}
      |	},
      |	"ds":{
      |		"dsType":"samplesrc",
      |		"fileType":"hive",
      |		"id":137,
      |   "target":"",
      |		"label":{
      |			"desc":"label",
      |			"name":"7d",
      |			"type":"number"
      |		}
      |	},
      |	"out":{
      |		"dst":"/model/${flowId}/${nodeId}/ds_out.csv"
      |	}
      |}""".stripMargin
  val sample_split =
    """
      |{
      |    "st":{
      |        "method":"random",
      |        "test_size":0.2,
      |        "random_state":7,
      |        "time_col":"apply_risk_created_at",
      |        "index_col":"apply_risk_id",
      |        "label_col":"d14",
      |        "py_split":"true"
      |    },
      |    "ds":"#state(${prevId},ds)",
      |    "out":{
      |        "dst":"/model/${flowId}/${nodeId}"
      |    }
      |}
    """.stripMargin

  val fea_transform =
    """
      |{
      |	"st": {
      |		"cate": [{
      |			"encoders": [{
      |				"method": "BaseEncoder",
      |				"params": {
      |					"cate_thr": 0.5,
      |					"missing_thr": 0.8,
      |					"same_thr": 0.9
      |				}
      |			},
      |			{
      |				"method": "CountEncoder",
      |				"params": {
      |					"log_transform": true,
      |					"unseen_value": 1,
      |					"smoothing": 1
      |				}
      |			}],
      |			"cols": []
      |		}],
      |		"method": "auto",
      |		"params": {
      |			"thr": 5
      |		},
      |		"cont": [{
      |			"encoders": [{
      |				"method": "BaseEncoder",
      |				"params": {
      |					"cate_thr": 0.5,
      |					"missing_thr": 0.8,
      |					"same_thr": 0.9
      |				}
      |			}],
      |			"cols": []
      |		}],
      |		"custom": [{
      |			"cols": ["equipment_app_name"],
      |			"encoders": [{
      |				"method": "AppCateEncoder",
      |				"params": {
      |					"cate_dict": "#cate_dict_path(2)",
      |					"delimiter": "|",
      |					"prefix": "app_",
      |					"unknown": "unknown"
      |				}
      |			}]
      |		}],
      |		"verbose": true
      |	},
      |	"ds": "#state(${prevId},ds)",
      |	"out": {
      |		"dst": "/model/${flowId}/${nodeId}"
      |	}
      |}
    """.stripMargin

  val train_filter =
    """
      |{
      |	"st": {
      |		"method": "SelectFromModel",
      |		"params": {
      |			"estimator": {
      |				"estimator": "XGBClassifier",
      |				"params": {
      |
      |				}
      |			},
      |			"threshold": "0.1*mean"
      |		}
      |	},
      |	"ds": "FEA_TRANSFORM",
      |	"out": {
      |		"dst": "TRAIN_TRAINER"
      |	}
      |}
    """.stripMargin

  val train_trainer =
    """
      |{
      |	"st": {
      |		"method": "XGBClassifier",
      |		"test_size": 0,
      |		"oversample": false,
      |		"n_folds": 5,
      |		"random_state": 7,
      |		"params": {
      |			"colsample_bytree": 0.8,
      |			"reg_lambda": 20,
      |			"silent": true,
      |			"base_score": 0.5,
      |			"scale_pos_weight": 1,
      |			"eval_metric": "auc",
      |			"max_depth": 1,
      |			"n_jobs": 1,
      |			"early_stopping_rounds": 30,
      |			"n_estimators": 1000,
      |			"random_state": 0,
      |			"reg_alpha": 1,
      |			"booster": "gbtree",
      |			"objective": "binary:logistic",
      |			"verbose": false,
      |			"colsample_bylevel": 0.8,
      |			"subsample": 0.7,
      |			"learning_rate": 0.1,
      |			"gamma": 0.5,
      |			"max_delta_step": 0,
      |			"min_child_weight": 10
      |		},
      |		"verbose": true
      |	},
      |	"ds": "TRAIN_FILTER",
      |	"out": {
      |		"dst": "model"
      |	}
      |}
    """.stripMargin

  def getJsonSr: String = {
    val json =  new JsonObject()
    json.addProperty("FEA_DS", fea_ds)
    json.addProperty("FEA_SAMPLE_SPLIT", sample_split)
    json.addProperty("FEA_TRANSFORM", fea_transform)
    json.addProperty("TRAIN_FILTER", train_filter)
    json.addProperty("TRAIN_TRAINER", train_trainer)
    json.toString
  }
}
