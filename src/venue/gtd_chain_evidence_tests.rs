use super::*;
use chrono::TimeZone;
use std::sync::Mutex;

struct FakeRpc {
    failure: Option<&'static str>,
    calls: Mutex<Vec<(String,String,Value)>>,
}
#[async_trait::async_trait]
impl RpcTransport for FakeRpc {
    async fn call(&self, url: &str, method: &str, params: Value) -> Result<Value,String> {
        self.calls.lock().unwrap().push((url.to_owned(),method.to_owned(),params.clone()));
        if self.failure==Some("timeout") && method=="eth_getLogs" {return Err("timeout".into());}
        let result=match method {
            "eth_blockNumber"=>json!("0x100"),
            "eth_getBlockByNumber"=>{
                let n=u64::from_str_radix(params[0].as_str().unwrap().trim_start_matches("0x"),16).unwrap();
                if self.failure==Some("null-head") && n==256 {
                    return Ok(json!({"jsonrpc":"2.0","id":1,"result":null}));
                }
                let digest=if self.failure==Some("disagree") && url=="https://rpc-b" {"b"} else {"a"};
                json!({"number":format!("0x{n:x}"),"hash":format!("0x{}",digest.repeat(64)),"timestamp":format!("0x{:x}",n*10)})
            }
            "eth_getLogs"=>{
                if self.failure==Some("null") {return Ok(json!({"jsonrpc":"2.0","id":1,"result":null}));}
                let address=params[0]["address"].as_str().unwrap();
                let exact=params[0]["topics"].as_array().unwrap().len()>1;
                if !exact && self.failure==Some("empty-control") {json!([])}
                else if exact && self.failure==Some("hash-fill") {json!([log(address,true)])}
                else if exact {json!([])} else {json!([log(address,false)])}
            }
            other=>panic!("unexpected RPC method {other}"),
        };
        Ok(json!({"jsonrpc":"2.0","id":1,"result":result}))
    }
}
fn log(address:&str,target:bool)->Value {
    let topic=if target {format!("0x{}","c".repeat(64))} else {format!("0x{}","f".repeat(64))};
    json!({"removed":false,"address":address,"blockNumber":"0xa","blockHash":format!("0x{}","a".repeat(64)),
        "transactionHash":format!("0x{}","d".repeat(64)),"logIndex":"0x0","topics":[TOPIC,topic],"data":"0x"})
}
fn setup(failure:Option<&'static str>)->(Rpc,Arc<FakeRpc>) {
    let fake=Arc::new(FakeRpc{failure,calls:Mutex::new(Vec::new())});
    (Rpc{transport:fake.clone(),endpoints:vec!["https://rpc-a".into(),"https://rpc-b".into()]},fake)
}
async fn scan(failure:Option<&'static str>)->(Result<ZeroProof,String>,Arc<FakeRpc>) {
    let (rpc,fake)=setup(failure);
    let outcome=rpc.prove_zero(ScanRequest{order_hash:&format!("0x{}","c".repeat(64)),
        maker:&format!("0x{}","b".repeat(40)),token:"123456",start:10,end:11,
        submitted:Utc.timestamp_opt(100,0).unwrap(),settled:Utc.timestamp_opt(110,0).unwrap()}).await;
    (outcome,fake)
}
#[tokio::test]
async fn zero_requires_two_nodes_two_exchanges_and_unfiltered_controls() {
    let (result,fake)=scan(None).await;
    assert!(result.is_ok(),"{result:?}");
    let calls=fake.calls.lock().unwrap();
    let logs:Vec<_>=calls.iter().filter(|(_,method,_)| method=="eth_getLogs").collect();
    assert_eq!(logs.len(),8);
    assert_eq!(logs.iter().filter(|(_,_,p)|p[0]["topics"].as_array().unwrap().len()==1).count(),4);
    assert!(EXCHANGES.iter().all(|address| logs.iter().any(|(_,_,p)|p[0]["address"]==*address)));
}
#[tokio::test]
async fn every_ambiguous_rpc_result_refuses_zero() {
    for failure in ["timeout","null","disagree","empty-control","hash-fill"] {
        let (result,_)=scan(Some(failure)).await;
        assert!(result.is_err(),"{failure} became zero evidence");
    }
}
#[tokio::test]
async fn binary_search_uses_finalized_upper_bound_when_latest_block_is_null() {
    let (rpc,fake)=setup(Some("null-head"));
    assert_eq!(rpc.first_block_at(110).await.unwrap(),11);
    let calls=fake.calls.lock().unwrap();
    assert!(!calls.iter().any(|(_,method,params)|method=="eth_getBlockByNumber" && params[0]=="0x100"));
}

#[tokio::test]
async fn binary_search_rejects_target_after_latest_finalized_block() {
    let (rpc,_)=setup(None);
    assert!(rpc.first_block_at(1281).await.unwrap_err().contains("尚未达到最终确认"));
}

#[tokio::test]
async fn insufficient_finality_refuses_zero_before_log_queries() {
    let (rpc,fake)=setup(None);
    let result=rpc.prove_zero(ScanRequest{order_hash:&format!("0x{}","c".repeat(64)),
        maker:&format!("0x{}","b".repeat(40)),token:"123456",start:10,end:129,
        submitted:Utc.timestamp_opt(100,0).unwrap(),settled:Utc.timestamp_opt(110,0).unwrap()}).await;
    assert!(result.unwrap_err().contains("finality"));
    assert!(!fake.calls.lock().unwrap().iter().any(|(_,m,_)|m=="eth_getLogs"));
}
