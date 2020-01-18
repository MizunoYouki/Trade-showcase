# 要求宣言型の冪等なトレーダー
要求を宣言し、その実現を待つトレーダーです。

この処理はスレッドセーフではありません。

この処理は冪等な性質を持ちます。

TODO: 中止シグナル？を受け取った場合、あるいはOODが観測された場合は、可能な限り速やかに処理が中断され、回復は行われません。
回復させたい場合は、冪等な性質を利用して、回復に必要な要求を宣言することが可能です。

裁量トレードとの併用は不可です（建玉や注文に関して、botと裁量を区別することができないため）。
    
## トレーダーの実行
    starategist.make_decision() -> requirements
    trader.queue.put(requirements)
        trader._sl_balancer.queue.put(requirements)
    

## 状態の遷移
この処理は、以下の状態を遷移します。

     clearing-orders
      |     すべての注文を空にしています。
      |     sl_chaserにも空の要求を渡します。
      |
      |     (getchildorders) 注文が空であることを確認する
      |     (getparentorders) ストップロス注文が空であることを確認する
      |
     making-orders
      |     要求を実現するために必要な注文を算出中です。
      |     - (getpositions) 建玉の一覧を取得する
      |       - （websocketも使うかもしれない）
      |
     ordering
      |     新規注文中です。
      |     with semaphore:
      |         - (sendchildorder) 新規注文する
      |         - (getchildorders) 注文が受け付けられたことを確認する
      |     sl_chaserに要求を渡します。
      |
     ordered
            要求を実現するための注文を完了した状態です

## シナリオ
### 要求が全く約定しない（または一部のみ約定した）
特になにも行われず、価格が注文価格に達して約定されるのを待つ。
価格が反対方向へ進みストップロス価格に達したら、その約定を待つ。

### state:orderedより前の過程で、新たな要求が宣言された(OOD: OutOfDateException)
可能な限り早く、かつ安全な範囲で、現在の処理が中止されます。
stateがclearing-ordersにされ、新たな要求の実現を図ります。

### 要求の一部のみ約定した状態で、ストップロス価格に達した
特になにも行われず、ストップロス注文の約定を待つ。

    要求を宣言した。
        requirements:
            side=Buy, price=100, size=10, amount=1000, stoploss_price=50
    
    要求の実現に必要な注文が送信された。
        childorders:
            side=Buy, price=100, size=10
    
    一部が約定した。
        remote_positions:
            side=Buy, amount=250
            
    この時要求は変わらないが、要求の実現に必要な注文は以下のように自動的に減算される。
        requirements:
            side=Buy, price=100, size=10, amount=1000, stoploss_price=50
        childorders:
            side=Buy, price=100, size=7.5
    
    SL-Chaserにより、ストップロス注文が送信された。
        parent_orders:
            side=Sell, price=50, size=2.5

    priceが50となり、全ストップロス注文が約定した。
    
    この時要求は変わらない。要求の実現に必要な注文も変わらない。
        requirements:
            side=Buy, price=100, size=10, amount=1000, stoploss_price=50
        childorders:
            side=Buy, price=100, size=7.5
  
### 要求が全て約定した後、ストップロス注文がすべて約定した
stateがemptyに移行される？

    要求を宣言した。
        requirements:
            side=Buy, price=100, size=10, amount=1000, stoploss_price=50
    
    要求の実現に必要な注文が送信された。
        childorders:
            side=Buy, price=100, size=10
    
    全て約定した。
        remote_positions:
            side=Buy, amount=1000
            
    要求の実現に必要な注文は空になっている。
        childorders:
            (empty)
    
    SL-Chaserにより、ストップロス注文が送信された。
        parent_orders:
            side=Sell, price=50, size=10

    priceが50となり、全てのストップロス注文が約定した。
    
    要求の実現に必要な注文は空になので、次の要求が宣言されるまでなにも行われない。
        childorders:
            (empty)
      
### ストップロスが完全に約定していない

    例: 建玉ポリシーを枚数:1とする
    
    100円で1枚Buyした。
    価格がtriggerである90円に達して、ストップロス注文が0.1枚約定すると、価格は85円になった。
    
    TODO: どうするか？もし85円でSignal.SELLが送出されたら、85円で1.9枚のSELL注文(doten)を出すので、また約定を待つことになる。
    TODO: またはカウントダウンでストップロス価格を緩くしていく
    
### REST APIが500番台
アラートが送出される。
成功するまで無限にリトライを試みる。

### API Limitに達した
アラートが送出される。
自動再開を待つか、botの終了どちらかを選択できる。
botを終了させた場合、人力でexiting/entering/entered/emptyのいずれかに相当する状態へ手動操作したのちは、botを開始させてよい。

    再びAPIを利用できるようになるまで待つ(=自動再開)ことは、考えられるひとつの手法であろう。
    しかし、どれだけ待てばよいか分からないし（5分以上？）、待っていられない場合もある。
    その場合に備えて手動でprocessingできることも必要である。
                        
    当初は、以下の処理を考案した。
    (待機 then ((自動再開) or (休止 then 手動操作 then 再開)) or OOD
    
    しかし手動操作ではOODが送出された時に即座に操作を中止することができないことを考えると、上記は採用できない。
    
    したがって、以下の処理が妥当だろう。
    (待機 then (自動再開 or botの終了)) or OOD
                        
    botを終了させた場合は、botを起動さｓて任意の要求を宣言してよい。


## 必要資金について
買い値と売り値が同じ場合、最大で注文価格の3倍資金が必要です。 TODO: 精査、LtoSとStoLで違うか？
（positionに対応するstoplossを注文する際は、position同額まで必要証拠金として加算されません？）


要求が完全に約定する（long to short)の場合、

    clearing-orders
    建玉および対応するストップロス注文のみになるよう、他の注文を削除中です。

        required total = ￥1000
            position = ￥1000 = 1*￥1000
            stoploss order = ￥900 = 1*￥900
        price: 1095

        position:
            side: buy
            size: 1
            price: 1000
        order:
            method: SIMPLE
                type: STOP_LIMIT
                side: sell
                size: 1
                trigger: 900
                price: 900

    - making-orders
    要求を実現するのに必要な注文を算出中です。

        required deposit = (save as above)
        price: 1095

    - ordering
    新規注文中です。

        required deposit = (save as above)
        price: 1095

    - exiting / entering
    注文が完了して、約定を待っている、あるいは約定中です。

        - 注文が完了して約定が始まっていない
            required total = ￥3200 = 1000 + 2200
                position = ￥1000 = 1*￥1000
                stoploss order = ￥900 = 1*￥900
                limit order = ￥2200 = 2*￥1100
            price: 1095

            position:
                side: buy
                size: 1
                price: 1000
            order:
                method: SIMPLE
                    type: STOP_LIMIT
                    side: sell
                    size: 1
                    trigger: 900
                    price: 900
                + method: SIMPLE
                +   type: LIMIT
                +   side: sell
                +   size: 2
                +   price: 1100

        - 一部約定している(state: exiting)
            required total = ￥2640 = 770 + 1870
                position = ￥770 = 0.7 * ￥1100
                stoploss order = ￥630 = 0.7 * ￥900
                limit order = ￥1870 = 1.7*￥1100
            price: 1100
            position:
                side: buy
                - size: 1
                + size: 0.7
                price: 1100
            order:
                method: SIMPLE
                    type: STOP_LIMIT
                    side: sell
                    - size: 1
                    + size: 0.7
                    trigger: 900
                    price: 900
                method: SIMPLE
                    type: LIMIT
                    side: sell
                    - size: 2
                    + size: 1.7
                    price: 1100

        - 一部約定している(state: entering)
            required total = ￥2830 = 880 + (960-880) + 1870
                position = ￥880 = 0.8 * ￥1100
                stoploss order = ￥960 = 0.8 * ￥1200
                limit order = ￥1870 = 1.7*￥1100
            price: 1100
            position:
                - side: buy
                + side: sell
                - size: 0.7
                + size: 0.8
                price: 1100
            order:
                method: SIMPLE
                    type: STOP_LIMIT
                    side: sell
                    - size: 0.7
                    + size: 0.8
                    trigger: 1200
                    price: 1200
                method: SIMPLE
                    type: LIMIT
                    side: sell
                    - size: 2
                    + size: 1.7
                    price: 1100

    - entered
    要求の実現が完了した状態です

        required total = ￥1200 = 1100 + (1200-1100)
            position = ￥1100 = 1*￥1100
            stoploss order = ￥1200 = 1*￥1200
        price: 1105
            position:
                side: sell
                size: 1
                price: 1100
            order:
                method: SIMPLE
                    type: STOP_LIMIT
                        side: buy
                        - size: 0.21
                        + size: 1
                        trigger: 1200
                        price: 1200
  
                        
## スレッドセーフではない理由
- g  : 親注文の取得（"->..."は、得られたparent_order_id）
- c  : 親注文を直列にキャンセル
- fet: 建玉情報の更新
- cal: 新たな建玉に必要な注文を算出
- s  : 新規に親注文
- con: 親注文が受け付けられたことを確認

- PN : 建玉

例：
Local-1スレッドと、Local-2スレッドで、同じ要求(size=0.01)を複数スレッドから並行（または並列）実行した場合、
下記シーケンスのようにP1だけでなくP2が作成されてしまう。
P1.sizeは0.01だが、P2.sizeは0.02となり、P2と要求が異なってしまう。

    Local-1:      | g->1 2 3 | c1 c2 c3 | g-None | fet   | cal | s             | con |
    BF     : | P0                                                       | P1        | P2
    Local-2:                              | g->None | cNone | fet | cal | s  | con |
