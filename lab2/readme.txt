pip install pyzmq

python generator.py
python worker.py >worker1.txt
python worker.py >worker2.txt
python worker.py >worker3.txt
python dashboard.py >generator.txt