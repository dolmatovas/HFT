# HFT
Repo for HFT project in CMF

## Quick start:
Load data using `load_md_from_file` function:
```
md = load_md_from_file(path=PATH_TO_FILE, nrows=NROWS)
```
Specify simulation latency and md_latency in nanoseconds:
```
latency = pd.Timedelta(10, 'ms').delta
md_latency = pd.Timedelta(10, 'ms').delta
```
Create simulator object:
```
sim = Sim(md, latency, md_latency)
```
Specify strategy parameters:
```
delay = pd.Timedelta(0.1, 's').delta
hold_time = pd.Timedelta(10, 's').delta
```
Create strategy object
```
strategy = BestPosStrategy(delay, hold_time)
```
Run simulation:
```
trades_list, md_list, updates_list, all_orders = strategy.run(sim)
```
Simulation results:
```
trades_list(List[OwnTrade]): list of executed trades
md_list(List[MdUpdate]): list of market data received by strategy
updates_list(List[Union[OwnTrade, MdUpdate]]): list of all updates received by strategy(market data and information about executed trades)
all_orders(List[Orted]): list of all placed orders
```
Use `get_pnl_funciton` to get PnL and info about positions in USD and BTC

```
df = get_pnl(updates_list)
PnL = df.total
```