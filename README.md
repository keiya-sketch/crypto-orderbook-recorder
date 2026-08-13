# crypto-orderbook-recorder

Binance の板情報(L2 order book)と約定を、**PCを閉じていても止まらない形**で記録するための収集基盤。

企画の背景: `AI-Team-Vault/プロジェクト/AI企画プロジェクト/仮想通貨FXAIシグナルツール/🛠️制作物/bizscout_仮想通貨FXAIシグナルツール.md`
（海外12社の競合調査 → 「執行品質(逆選択・スリッページ)の可視化」が誰もやっていない差別化軸だと判明 → PolyMarket bot で確立した手法が暗号資産CEXに技術的に移植可能か検証した結果、このリポジトリを作成）

**⚠️ このリポジトリはコードのみ用意した状態です。まだ git init / push / Actions の起動は一切行っていません。** 実行するかどうかはKei判断。

---

## ⚠️ 先に読んでほしいコスト試算(実行前に必ず確認)

毎時50分間の記録を24時間×30日続けると、GitHub Actions の消費時間は概算で **月間 約36,000分**（24回/日 × 50分 × 30日）になる。

| リポジトリ種別 | 無料枠 | 超過分の料金 | この使い方での概算月額 |
|---|---|---|---|
| **Public（公開）** | **無制限・無料** | — | **$0** |
| Private（非公開）・Free プラン | 2,000分/月 | $0.006/分（2026年1月改定後） | 約 **$204/月** |
| Private・Team プラン | 3,000分/月 | 同上 | 約 **$198/月** |

出典: [GitHub Actions Pricing 2026](https://cicdcost.com/github-actions-pricing) / [Free Tier Limits](https://cicdcalculator.com/github-actions-free-tier)（2026-08-12時点の公開情報から試算。実測ではない）

**このリポジトリのコードには取引所APIキーや秘密情報は一切含まれない**（板・約定はどちらも認証不要の公開マーケットデータ）。実際に使うトレード戦略・シグナルロジックはこのリポジトリの範囲外（Silver/Gold層はMac側で別途構築する想定）なので、**Publicにしても手口や資金は漏れない**。コスト面ではPublicが妥当と考えるが、最終判断はKeiに委ねる。

Privateのまま使う場合は、`schedule` の cron 頻度や `duration_seconds` を下げて消費時間を抑えること（例: 3時間おき・20分間の記録に減らせば月間消費は約2,880分まで下がる）。

---

## 設計原則(`investment-validation` リポジトリを踏襲)

| 原則 | 理由 |
|---|---|
| **データをコミットしない** | マスターは GDrive。リポジトリはコードのみ |
| **常駐プロセスを作らない** | Actions の1回の実行内で完結する有限時間バッチ(既定50分)。VPSは使わない |
| **重い処理をサーバーに置かない** | 収集のみ Actions、逆選択・スリッページの分析は Mac 側で行う(未着手) |
| **べき等にする** | 同じ1時間パーティションを何度取得しても上書きされるだけ |
| **生データ(Bronze)を消さない** | 後から分析ロジックを作り直せる |

## データの流れ

**2026-08-13統合**: `investment-validation` リポジトリが既に集めている Binance klines(1分足OHLCV、BTCUSDT/ETHUSDT、2026-08-07〜)と同じ Bronze データルートに統合した。板・約定はコード(このリポジトリ)は分離したまま、出力先だけ揃えている。

```
GitHub Actions (毎時50分間、板+約定をWSで記録)                    ← このリポジトリ
GitHub Actions (日次、klines 1分足を記録)                        ← investment-validation リポジトリ
  → どちらも Bronze Parquet (ローカル一時ファイル)
  → rclone で同じ GDrive ルートへアップロード
     AI-Team-Vault/プロジェクト/投資検証基盤/data/
       bronze/binance/klines/symbol=.../interval=1m/date=.../data.parquet      ← investment-validation
       bronze/binance/orderbook/symbol=.../date=.../hour=.../data.parquet      ← このリポジトリ
       bronze/binance/trades/symbol=.../date=.../hour=.../data.parquet         ← このリポジトリ
       ledger/orderbook_runs/date=.../run_id=....parquet                      ← このリポジトリ
  → Drive for Desktop が自動同期
  → Mac 側で Silver/Gold へ変換し、klines・板・約定を突き合わせて逆選択・スリッページを実測(未着手)
```

## 板の再構成手順

Binance 公式の手順に準拠(詳細は `src/ingest/binance_orderbook.py` の docstring 参照)。
WebSocket で diff イベントを受け続けながら REST snapshot を取り、整合性が崩れたら自動で resync する。

エンドポイントは地域制限のない market-data 専用ドメインを既定で使う:
- REST: `https://data-api.binance.vision`
- WS: `wss://data-stream.binance.vision`

(`api.binance.com` / `stream.binance.com` は GitHub Actions のランナー(主に米国IP)から451が返ることが `investment-validation` の `binance_klines.py` で実測済みのため)

## 既知の制約

- **時間帯の境目に短いギャップが生じる**: 1回の実行は50分。次の実行までの約10分間(セットアップ・アップロード時間)は記録が途切れる。逆選択の実測では、この間のイベントは欠測として扱う必要がある。
- **REST snapshot は最大1000階層まで**: `depth` パラメータの上限。極端に厚い板の裾は捉えられない。
- **記録するスナップショット間隔は1秒**: 生の diff イベントは100ms単位だが、板スナップショットとして保存するのは1秒に1回(ファイルサイズ抑制のため)。約定(trade)は全件記録するので、逆選択分析の主軸は「約定 vs 直近1秒以内の板」になる。PolyMarketで見られたミリ秒単位の遅延影響までは捉えられない可能性がある。
- **FXは対象外**: 分散型OTC市場で単一の中央板が存在しないため、この手法は原理的に移植できない(企画書の技術検証セクション参照)。
- **2026-08-13修正済み**: 時間パーティションが「フラッシュ時刻」基準だと隣の hour= フォルダにデータが漏れるバグを実データで発見・修正した(イベント時刻でグルーピングする方式に変更)。修正前に書かれた一部ファイルは境界付近で数分ぶんのズレを含みうる。分析時は必ず`hour=`フォルダ名ではなく`ts`列を正とすること。

## 検証方針(The Fool検証・2026-08-13確定)

設計レビュー(前提の暴露→反対意見・攻撃)で確定した運用ルール:

- **卒業基準**: 7日間・resync率が異常でない・欠測が1日あたり2時間未満、を満たした時点で一度分析に着手する(「もっとデータを溜めてから」で無期限に先送りしない)
- **判定構造の事前固定**: 分析時は必ず時間帯シャッフルのプラセボ対照とセットで行う。閾値の数字はデータを見てから決めてよいが、判定の型は先に固定する
- **実測で早くも判明**: BTCUSDTのスプレッドは$63,614に対し$0.01(約0.16bp)しかなく、執行コストが無視できるほど小さい可能性が高い。卒業基準を待たず、板が薄いアルトコインへの展開を前倒しで検討する価値がある
- **執行基盤とシグナル探索は並行してよい**: 執行コスト測定の完成を待たず、既存klinesデータでのシグナル仮説検討を並行して始めて構わない

## セットアップ手順(実施済み)

2026-08-12にpublicリポジトリとして稼働開始済み: https://github.com/keiya-sketch/crypto-orderbook-recorder

再現する場合の手順:

```bash
cd /Users/tk/projects/crypto-orderbook-recorder
git init
git add -A
git commit -m "initial commit"

# Public/Private はコスト試算を踏まえて判断(このリポジトリはpublicで運用中)
gh repo create <repo名> --public --source=. --push
# または: gh repo create <repo名> --private --source=. --push

# rclone の GDrive 設定を Secret として登録(investment-validation と同じ手順)
gh secret set RCLONE_CONFIG_GDRIVE < ~/.config/rclone/rclone.conf
```

Actions は push した時点で有効化され、次の毎時0分から `record-orderbook.yml` が起動する。
手動で1回だけ試す場合は `workflow_dispatch`(Actionsタブから「Run workflow」)で `duration_seconds` を短め(例: 120)にして動作確認するとコストを抑えられる。

## ローカル実行(未実行・動作確認用のコマンド例)

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

# BTCUSDT を2分間だけ試しに記録する例
.venv/bin/python -m src.ingest.binance_orderbook --symbols BTCUSDT --duration-seconds 120 --out ./data
```

## 次のステップ(このリポジトリの範囲外)

1. 実際に数日〜1週間分のデータを溜める
2. Mac側で Silver/Gold 変換 + 逆選択・スリッページの実測(想定発注サイズでの板コスト計算)
3. BTC/ETHのようなメジャーペアで本当に効果があるかを確認してから、アルトコイン等の薄い板に対象を広げるか判断する
