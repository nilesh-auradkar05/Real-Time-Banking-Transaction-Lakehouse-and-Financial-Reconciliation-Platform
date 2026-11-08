import json
from banking_sink.events import parse_event

def test_czech_lineage_contract():
    obj={"op":"u", "source":{"table":"fin_trans", "lsn":123,"ts_ms":1700000000000},
         "after":{"trans_id":44,"account_id":8,"data_origin":"faker",
                  "synthetic_batch_id":7,"last_replay_event_id":1234}}
    e=parse_event(topic="banking_server.public.fin_trans",partition=1,offset=9,
                  kafka_timestamp_ms=1700000000000,key=b'{"trans_id":44}',value=json.dumps(obj).encode())
    assert (e.record_pk,e.replay_event_id,e.data_origin,e.synthetic_batch_id)==(44,1234,"faker",7)
    assert e.event_id == "banking_server.public.fin_trans:1:9"

def test_delete_lineage_from_before():
    e=parse_event(topic="banking_server.public.fin_trans",partition=0,offset=2,
        kafka_timestamp_ms=None,key=b'{"trans_id":1}',
        value=json.dumps({"op":"d","source":{"table":"fin_trans"},"before":{"trans_id":1,"data_origin":"czech"}}).encode())
    assert e.record_pk==1 and e.data_origin=="czech"
