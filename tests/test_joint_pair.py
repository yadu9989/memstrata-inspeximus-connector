import hashlib
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from examples.prepare_joint_fixture import prepare
from examples.run_joint_pair import run, verify_bundle
from memstrata_mnemo_connector import DeliveryCredentials


def test_pair_runner_real_http_and_resume(tmp_path):
    bundle=tmp_path/"bundle";prepare(bundle)
    received, calls = {}, []
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def do_POST(self):
            raw=self.rfile.read(int(self.headers["Content-Length"]))
            x=json.loads(raw)["fact_record"]; key=x["id"]; digest=hashlib.sha256(raw).hexdigest()
            calls.append((key,digest))
            if key in received and received[key]["payload_sha256"]!=digest:
                self.send_response(409);self.end_headers();return
            if key not in received:
                received[key]={"source_id":key,"payload_sha256":digest,"status":"committed",
                               "transaction_id":"txn-"+key,"ledger_record_id":"ledger-"+key,"committed_at":1001.0}
            self.send_response(200);self.end_headers();self.wfile.write(json.dumps(received[key]).encode())
    server=ThreadingHTTPServer(("127.0.0.1",0),Handler)
    thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:
        endpoint=f"http://127.0.0.1:{server.server_port}"
        result=run(bundle,tmp_path/"run",endpoint,DeliveryCredentials("synthetic-bearer"),loopback=True)
        assert result["delivery_checks_passed"] and not result["joint_test_complete"]
        assert len(calls)==5 and len(received)==2
        assert run(bundle,tmp_path/"run",endpoint,DeliveryCredentials("synthetic-bearer"),loopback=True)==result
        assert len(calls)==5  # No resend of completed steps on script restart.
    finally:
        server.shutdown();server.server_close();thread.join()


def test_changed_bundle_fails_before_dispatch(tmp_path):
    bundle=tmp_path/"bundle";prepare(bundle)
    (bundle/"before.json").write_bytes((bundle/"before.json").read_bytes()+b" ")
    with pytest.raises(ValueError,match="hash"):verify_bundle(bundle)


def test_prepare_never_overwrites_existing_fixture(tmp_path):
    bundle=tmp_path/"bundle";prepare(bundle)
    before=(bundle/"before.json").read_bytes()
    with pytest.raises(FileExistsError):prepare(bundle)
    assert (bundle/"before.json").read_bytes()==before
