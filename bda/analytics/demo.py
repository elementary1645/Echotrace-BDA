"""Weight-free professor demonstration. Uses the existing simulated acoustic pipeline."""
import argparse
from pathlib import Path
import sys


def main():
    sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'backend'))
    from echotrace.api.server import Engine, create_app
    import uvicorn
    p=argparse.ArgumentParser()
    p.add_argument('--port',type=int,default=8000)
    p.add_argument('--kafka',action='store_true',help='Opt in to existing localhost Kafka infrastructure')
    args=p.parse_args()
    if args.kafka:
        import os
        os.environ['KAFKA_ENABLED']='true'
    # Suppresses speculative neural preloading for this weight-free demo only.
    # LIVE/RECORDED remain supported by the normal backend/scripts/serve.py launcher.
    def no_weights():
        raise RuntimeError('Weight-free demonstration: use the normal server launcher for LIVE/RECORDED')
    app=create_app(Engine(tagger_factory=no_weights),{'source':'SIMULATED','scenario':'breakin'})
    print(f'Clearly SIMULATED acoustic demo: http://127.0.0.1:{args.port}',flush=True)
    uvicorn.run(app,host='127.0.0.1',port=args.port)


if __name__=='__main__':
    main()
