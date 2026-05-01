import { useEffect, useMemo, useState } from "react";
import {
  Activity,
  AlertTriangle,
  BarChart3,
  ChevronDown,
  Database,
  ExternalLink,
  FileSearch,
  LineChart as LineChartIcon,
  RefreshCw,
  Search,
} from "lucide-react";
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Line,
  LineChart,
  ReferenceLine,
  ResponsiveContainer,
  Scatter,
  ScatterChart,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import {
  createResearchRun,
  fetchAnalysis,
  fetchOverview,
  fetchResearchRun,
  fetchSectors,
  fetchViralAnalysis,
} from "./api";
import { compactDate, integer, number, percent, shortTitle, signalLabels } from "./format";

const PAGES = [
  { key: "overview", label: "Overview", icon: Activity },
  { key: "analysis", label: "Deep Analysis", icon: BarChart3 },
  { key: "research", label: "Research Tool", icon: FileSearch },
];

const SIGNALS = ["pub_zscore", "pub_deviation", "pub_4w_dev"];
const CHART_MARGIN = { top: 10, right: 24, bottom: 28, left: 34 };
const COMPACT_CHART_MARGIN = { top: 10, right: 20, bottom: 28, left: 28 };

function useRemote(loader, deps) {
  const [state, setState] = useState({ data: null, loading: true, error: null });

  useEffect(() => {
    let cancelled = false;
    setState((current) => ({ ...current, loading: true, error: null }));
    loader()
      .then((data) => {
        if (!cancelled) setState({ data, loading: false, error: null });
      })
      .catch((error) => {
        if (!cancelled) setState({ data: null, loading: false, error });
      });
    return () => {
      cancelled = true;
    };
  }, deps);

  return state;
}

export default function App() {
  const [selectedSector, setSelectedSector] = useState("ai_tech");
  const [page, setPage] = useState("overview");
  const [signal, setSignal] = useState("pub_zscore");

  const sectorsState = useRemote(fetchSectors, []);
  const sectors = sectorsState.data?.sectors || [];
  const activeSector = sectors.find((sector) => sector.sector === selectedSector);

  useEffect(() => {
    if (!activeSector && sectors.length) {
      setSelectedSector(sectors[0].sector);
    }
  }, [activeSector, sectors]);

  return (
    <div className="app-shell">
      <header className="topbar">
        <div className="brand-block">
          <Database size={24} aria-hidden="true" />
          <div>
            <h1>LitMarket</h1>
            <span>Literature momentum and viral-paper market context</span>
          </div>
        </div>

        <div className="top-controls">
          <label className="select-label">
            Sector
            <span className="select-wrap">
              <select
                value={selectedSector}
                onChange={(event) => setSelectedSector(event.target.value)}
                disabled={!sectors.length}
              >
                {sectors.map((sector) => (
                  <option key={sector.sector} value={sector.sector}>
                    {sector.label}
                  </option>
                ))}
              </select>
              <ChevronDown size={16} aria-hidden="true" />
            </span>
          </label>
        </div>
      </header>

      <nav className="page-tabs" aria-label="Pages">
        {PAGES.map((item) => {
          const Icon = item.icon;
          return (
            <button
              key={item.key}
              className={page === item.key ? "active" : ""}
              type="button"
              onClick={() => setPage(item.key)}
            >
              <Icon size={17} aria-hidden="true" />
              {item.label}
            </button>
          );
        })}
      </nav>

      <main>
        {sectorsState.loading && <LoadingBlock />}
        {sectorsState.error && <ErrorBlock error={sectorsState.error} />}
        {!sectorsState.loading && !sectorsState.error && page === "overview" && (
          <OverviewPage sector={selectedSector} signal={signal} />
        )}
        {!sectorsState.loading && !sectorsState.error && page === "analysis" && (
          <AnalysisPage
            sector={selectedSector}
            signal={signal}
            onSignalChange={setSignal}
          />
        )}
        {!sectorsState.loading && !sectorsState.error && page === "research" && (
          <ResearchPage sector={activeSector} />
        )}
      </main>
    </div>
  );
}

function OverviewPage({ sector, signal }) {
  const state = useRemote(() => fetchOverview(sector, signal), [sector, signal]);
  const viralAnalysisState = useRemote(() => fetchViralAnalysis(sector), [sector]);
  if (state.loading) return <LoadingBlock />;
  if (state.error) return <ErrorBlock error={state.error} />;

  const data = state.data;
  const latestPublication = data.latest_publication || {};
  const latestReturn = data.latest_return || {};
  const weekly = data.weekly_evidence || {};
  const viral = data.viral_radar || {};
  const viralConclusion = viralAnalysisState.data?.conclusion;
  const viralDay5 = viralAnalysisState.data?.control_test?.day_5;
  const displaySparkline = filterDisplayWeeks(data.sparkline || []);
  const displayLatest = displaySparkline.at(-1) || {};

  return (
    <div className="page-stack">
      <div className="context-strip">
        This view shows the latest seeded sector state: current publication volume,
        latest weekly ETF return, and recent viral-paper detections. Signals are
        descriptive evidence, not trading instructions.
      </div>
      <section className="summary-band">
        <div className="sector-heading">
          <p>{data.sector.weekly_ticker} weekly ETF | {data.sector.viral_ticker} viral ETF</p>
          <h2>{data.sector.label}</h2>
          <div className="keyword-line">
            {data.sector.keywords.map((keyword) => (
              <span key={keyword}>{keyword}</span>
            ))}
          </div>
        </div>
        <div className="status-grid">
          <MetricCard
            label="Publication z-score"
            value={number(displayLatest.value ?? latestPublication.pub_zscore, 2)}
            detail={`${integer(displayLatest.pub_count ?? latestPublication.pub_count)} papers in the latest full week`}
          />
          <MetricCard
            label="ETF weekly return"
            value={percent(latestReturn.log_return)}
            detail={`${percent(latestReturn.abnormal_return)} abnormal return`}
          />
          <MetricCard
            label="Publication-market evidence"
            value={weekly.status}
            detail="Whether similar weekly publication surges historically preceded ETF abnormal returns"
          />
          <MetricCard
            label="Viral radar"
            value={viral.status}
            detail={viralConclusion?.text || `${viral.events.length} papers in the recent seeded window`}
          />
        </div>
      </section>

      <section className="overview-grid">
        <Panel title="Publication Signal" icon={LineChartIcon}>
          <p className="panel-note">
            Publication signal is the latest weekly paper volume measured against
            its own history. The z-score compares this week with a trailing
            52-week baseline.
          </p>
          <div className="chart-block short">
            <ResponsiveContainer>
              <AreaChart data={displaySparkline} margin={COMPACT_CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="week_start"
                  tickFormatter={(value) => value.slice(5)}
                  label={{ value: "Week start", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  width={76}
                  label={{ value: "Signal", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip />
                <Area
                  type="monotone"
                  dataKey="value"
                  stroke="#1c7c74"
                  fill="#b7ddd6"
                  strokeWidth={2}
                  name={signalLabels[signal]}
                />
              </AreaChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="Current Reading" icon={RefreshCw}>
          <dl className="update-list">
            <div>
              <dt>Latest publication week</dt>
              <dd>{compactDate(displayLatest.week_start || latestPublication.week_start)}</dd>
            </div>
            <div>
              <dt>Weekly evidence means</dt>
              <dd>{weekly.signal_is_elevated ? "Current signal is elevated" : "No current surge signal"}</dd>
            </div>
            <div>
              <dt>Control-test CAR+5 gap</dt>
              <dd>{viralDay5 ? percent(viralDay5.difference, 3) : "n/a"}</dd>
            </div>
            <div>
              <dt>Control-test p-value</dt>
              <dd>{viralDay5 ? number(viralDay5.p_value, 4) : "n/a"}</dd>
            </div>
          </dl>
        </Panel>
      </section>

      <Panel title="Viral Radar Feed" icon={Activity}>
        <ViralFeed events={viral.events} />
      </Panel>
    </div>
  );
}

function AnalysisPage({ sector, signal, onSignalChange }) {
  const [tab, setTab] = useState("momentum");
  const analysisState = useRemote(() => fetchAnalysis(sector, signal), [sector, signal]);
  const viralState = useRemote(() => fetchViralAnalysis(sector), [sector]);

  return (
    <div className="page-stack">
      <div className="analysis-controls">
        <div className="segmented" role="tablist" aria-label="Analysis tabs">
          <button
            className={tab === "momentum" ? "active" : ""}
            type="button"
            onClick={() => setTab("momentum")}
          >
            Publication Momentum
          </button>
          <button
            className={tab === "viral" ? "active" : ""}
            type="button"
            onClick={() => setTab("viral")}
          >
            Viral Event Study
          </button>
        </div>

        {tab === "momentum" && (
          <label className="select-label compact">
            Signal
            <span className="select-wrap">
              <select value={signal} onChange={(event) => onSignalChange(event.target.value)}>
                {SIGNALS.map((item) => (
                  <option key={item} value={item}>
                    {signalLabels[item]}
                  </option>
                ))}
              </select>
              <ChevronDown size={16} aria-hidden="true" />
            </span>
          </label>
        )}
      </div>

      {tab === "momentum" && <MomentumTab state={analysisState} signal={signal} />}
      {tab === "viral" && <ViralAnalysisTab state={viralState} />}
    </div>
  );
}

function MomentumTab({ state, signal }) {
  if (state.loading) return <LoadingBlock />;
  if (state.error) return <ErrorBlock error={state.error} />;

  const { result, series } = state.data;
  let runningAbnormal = 0;
  const weeklySeries = filterDisplayWeeks(series).map((row) => {
    runningAbnormal += row.abnormal_return || 0;
    return { ...row, cumulative_abnormal_return: runningAbnormal };
  });
  const lagRows = result.lag_correlation || [];
  const granger = result.granger || [];
  const carRows = normalizeCarRows(result.car);
  const signalStationary = result.adf?.signal?.is_stationary;

  return (
    <div className="page-stack">
      {signalStationary === false && (
        <div className="warning-strip">
          <AlertTriangle size={18} aria-hidden="true" />
          Signal ADF p-value {number(result.adf?.signal?.p_value, 4)}; Granger precedence may be unreliable.
        </div>
      )}

      <Panel title="Publication Count and ETF Abnormal Return" icon={LineChartIcon}>
        <p className="panel-note">
          The upper chart shows weekly paper count. The lower chart uses the same
          week axis and shows cumulative abnormal ETF return, after adjusting for
          SPY with the stored market model.
        </p>
        <div className="chart-pair">
          <div className="chart-block pair">
            <ResponsiveContainer>
              <ComposedChart data={weeklySeries} margin={CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="week_start"
                  tickFormatter={(value) => value.slice(2, 7)}
                  minTickGap={24}
                  label={{ value: "Week start", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  width={82}
                  label={{ value: "Papers", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip />
                <Bar dataKey="pub_count" fill="#d8b45f" name="Weekly paper count" />
              </ComposedChart>
            </ResponsiveContainer>
          </div>
          <div className="chart-block pair">
            <ResponsiveContainer>
              <LineChart data={weeklySeries} margin={CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="week_start"
                  tickFormatter={(value) => value.slice(2, 7)}
                  minTickGap={24}
                  label={{ value: "Week start", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  width={82}
                  tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                  label={{ value: "CAR", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip formatter={(value) => percent(value, 2)} />
                <Line
                  type="monotone"
                  dataKey="cumulative_abnormal_return"
                  stroke="#8561a6"
                  dot={false}
                  strokeWidth={2}
                  name="Cumulative abnormal return"
                />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      </Panel>

      <Panel title="Stored Publication Signal" icon={Search}>
        <p className="panel-note">
          Current selected signal: {signalLabels[signal]}. This panel keeps the
          transformed signal available for inspection without mixing it into the
          paper-count chart.
        </p>
        <div className="chart-block medium">
          <ResponsiveContainer>
            <LineChart data={weeklySeries} margin={CHART_MARGIN}>
              <CartesianGrid strokeDasharray="3 3" />
              <XAxis
                dataKey="week_start"
                tickFormatter={(value) => value.slice(2, 7)}
                minTickGap={24}
                label={{ value: "Week start", position: "insideBottom", offset: -2 }}
              />
              <YAxis
                width={82}
                label={{ value: "Signal", angle: -90, position: "insideLeft", offset: 0 }}
              />
              <Tooltip />
              <Line
                type="monotone"
                dataKey="selected_signal"
                stroke="#1c7c74"
                dot={false}
                strokeWidth={2}
                name={signalLabels[signal]}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      </Panel>

      <section className="two-column">
        <Panel title="Lag Correlation" icon={BarChart3}>
          <p className="panel-note">
            Each bar is a weekly lag. Positive lags ask whether publication
            movement comes before later abnormal ETF return. p-values test
            whether the correlation differs from zero.
          </p>
          <div className="chart-block medium">
            <ResponsiveContainer>
              <BarChart data={lagRows} margin={COMPACT_CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <XAxis
                  dataKey="lag"
                  label={{ value: "Lag in weeks", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  width={74}
                  label={{ value: "Pearson r", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip content={<LagTooltip />} />
                <Bar dataKey="pearson_r" name="Pearson r">
                  {lagRows.map((row) => (
                    <Cell key={row.lag} fill={lagColor(row)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="CAR After Surges" icon={Activity}>
          <p className="panel-note">
            CAR means cumulative abnormal return after publication-signal surge
            weeks. Significance depends on the stored p-values for each window.
          </p>
          {carRows.length ? (
            <div className="chart-block medium">
              <ResponsiveContainer>
                <BarChart data={carRows} margin={COMPACT_CHART_MARGIN}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="window"
                    label={{ value: "Weeks after surge", position: "insideBottom", offset: -2 }}
                  />
                  <YAxis
                    width={74}
                    tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                    label={{ value: "Mean CAR", angle: -90, position: "insideLeft", offset: 0 }}
                  />
                  <Tooltip content={<CarSurgeTooltip />} />
                  <Bar dataKey="mean_car" name="Mean CAR">
                    {carRows.map((row) => (
                      <Cell key={row.window} fill={row.significant ? "#8f3f71" : "#8561a6"} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          ) : (
            <EmptyState text="No CAR rows in the stored result." />
          )}
        </Panel>
      </section>

      <section className="two-column">
        <Panel title="Granger Result" icon={Search}>
          <p className="panel-note">
            Granger tests whether past publication signal improves prediction of
            weekly abnormal return. It is predictive precedence, not proof of
            causality.
          </p>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Lag</th>
                  <th>F-stat</th>
                  <th>p-value</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {granger.map((row) => (
                  <tr key={row.lag}>
                    <td>{row.lag}</td>
                    <td>{number(row.f_stat, 4)}</td>
                    <td>{number(row.p_value, 4)}</td>
                    <td>{row.sig_bonf ? "Bonferroni" : row.sig_05 ? "p < 0.05" : "n.s."}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </Panel>

        <Panel title="Stored Result Summary" icon={Database}>
          <dl className="update-list">
            <div>
              <dt>Observations</dt>
              <dd>{integer(result.n_obs)}</dd>
            </div>
            <div>
              <dt>Date range</dt>
              <dd>{compactDate(result.date_range?.[0])} to {compactDate(result.date_range?.[1])}</dd>
            </div>
            <div>
              <dt>Best lag</dt>
              <dd>{result.best_lag_corr?.lag ?? "n/a"} weeks, r {number(result.best_lag_corr?.pearson_r, 4)}</dd>
            </div>
            <div>
              <dt>Surge threshold</dt>
              <dd>{number(result.car_threshold, 3)} with {integer(result.car_n_events)} events</dd>
            </div>
          </dl>
        </Panel>
      </section>
    </div>
  );
}

function ViralAnalysisTab({ state }) {
  if (state.loading) return <LoadingBlock />;
  if (state.error) return <ErrorBlock error={state.error} />;

  const data = state.data;
  const car5 = data.car_5d_distribution?.events || [];
  const scatter = car5.filter((row) => row.car_5d !== null && row.reddit_hits !== null);
  const stats = data.car_5d_distribution?.stats || {};
  const control = data.control_test || {};
  const controlDay5 = control.day_5 || {};
  const conclusion = data.conclusion || {};

  return (
    <div className="page-stack">
      <div className={`conclusion-strip ${conclusion.direction === "increase" ? "positive" : conclusion.direction === "decrease" ? "negative" : ""}`}>
        {conclusion.text || "No viral-paper conclusion is available for this sector."}
        {controlDay5.p_value !== undefined && (
          <span>
            CAR+5 gap {percent(controlDay5.difference, 3)}, p-value {number(controlDay5.p_value, 4)}.
          </span>
        )}
      </div>
      <section className="two-column">
        <Panel title="Real Events vs Randomized Control" icon={LineChartIcon}>
          <p className="panel-note">
            This mirrors the control-test logic: real viral-paper dates are
            compared with randomized trading dates from the same sector price
            history. The y-axis is mean cumulative abnormal return.
          </p>
          <div className="chart-block medium">
            <ResponsiveContainer>
              <LineChart data={control.curve || []} margin={CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <ReferenceLine x={0} stroke="#101718" strokeWidth={2} />
                <ReferenceLine y={0} stroke="#101718" strokeWidth={2} />
                <XAxis
                  dataKey="day_relative"
                  label={{ value: "Trading days relative to event", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  width={82}
                  tickFormatter={(value) => `${(value * 100).toFixed(1)}%`}
                  label={{ value: "Mean CAR", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip formatter={(value) => percent(value, 3)} />
                <Line type="monotone" dataKey="real_mean_car" stroke="#1c7c74" strokeWidth={2} name="Real viral events" />
                <Line type="monotone" dataKey="control_mean_car" stroke="#6f7775" strokeDasharray="5 5" strokeWidth={2} name="Randomized control dates" />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="CAR+5 Distribution" icon={BarChart3}>
          <p className="panel-note">
            CAR+5 is cumulative abnormal return five trading days after the
            viral-paper publication date. The p-value tests whether the mean
            differs from zero.
          </p>
          <div className="metric-row">
            <MetricCard label="Events" value={integer(stats.n)} detail="Complete +5 windows" />
            <MetricCard label="Mean CAR+5" value={percent(stats.mean, 3)} detail={`p-value ${number(stats.p_value, 4)} | ${stats.significance || "n/a"}`} />
          </div>
          <div className="distribution-list">
            {car5.slice(0, 8).map((row) => (
              <div key={row.viral_event_id} className="distribution-row">
                <span>{shortTitle(row.title, 64)}</span>
                <strong>{percent(row.car_5d, 3)}</strong>
              </div>
            ))}
          </div>
        </Panel>
      </section>

      <section className="two-column">
        <Panel title="Attention vs CAR+5" icon={Activity}>
          <p className="panel-note">
            Each dot is one viral paper. Hover to inspect the paper title,
            Reddit hits, and CAR+5.
          </p>
          <div className="chart-block medium">
            <ResponsiveContainer>
              <ScatterChart margin={CHART_MARGIN}>
                <CartesianGrid strokeDasharray="3 3" />
                <ReferenceLine y={0} stroke="#101718" strokeWidth={2} />
                <XAxis
                  dataKey="reddit_hits"
                  name="Reddit hits"
                  type="number"
                  label={{ value: "Reddit DOI mentions", position: "insideBottom", offset: -2 }}
                />
                <YAxis
                  dataKey="car_5d"
                  name="CAR+5"
                  tickFormatter={(value) => `${(value * 100).toFixed(0)}%`}
                  width={82}
                  label={{ value: "CAR+5", angle: -90, position: "insideLeft", offset: 0 }}
                />
                <Tooltip content={<PaperScatterTooltip />} />
                <Scatter data={scatter} fill="#8561a6" name="Events" />
              </ScatterChart>
            </ResponsiveContainer>
          </div>
        </Panel>

        <Panel title="Volatility Event Study" icon={Search}>
          <p className="panel-note">
            Absolute abnormal return is the size of abnormal ETF movement
            regardless of direction. This compares average absolute abnormal
            return before and after viral-paper dates.
          </p>
          <dl className="update-list">
            <div>
              <dt>Events</dt>
              <dd>{integer(data.volatility_event_study?.stats?.n)}</dd>
            </div>
            <div>
              <dt>Pre-event absolute abnormal return</dt>
              <dd>{percent(data.volatility_event_study?.stats?.mean_pre_abs_ar, 3)}</dd>
            </div>
            <div>
              <dt>Post-event absolute abnormal return</dt>
              <dd>{percent(data.volatility_event_study?.stats?.mean_post_abs_ar, 3)}</dd>
            </div>
            <div>
              <dt>Mean change</dt>
              <dd>{percent(data.volatility_event_study?.stats?.mean_change, 3)}</dd>
            </div>
            <div>
              <dt>Paired-test p-value</dt>
              <dd>{number(data.volatility_event_study?.stats?.paired_t_test?.p_value, 4)}</dd>
            </div>
          </dl>
        </Panel>
      </section>
    </div>
  );
}

function PaperScatterTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <strong>{shortTitle(row.title, 90)}</strong>
      <span>Reddit hits: {integer(row.reddit_hits)}</span>
      <span>CAR+5: {percent(row.car_5d, 3)}</span>
    </div>
  );
}

function LagTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <strong>Lag {row.lag} weeks</strong>
      <span>Pearson r: {number(row.pearson_r, 4)}</span>
      <span>p-value: {number(row.pearson_p, 4)}</span>
      <span>{row.sig_bonf ? "Bonferroni significant" : row.sig_05 ? "p < 0.05" : "not significant"}</span>
    </div>
  );
}

function CarSurgeTooltip({ active, payload }) {
  if (!active || !payload?.length) return null;
  const row = payload[0].payload;
  return (
    <div className="chart-tooltip">
      <strong>+{row.window} weeks</strong>
      <span>Mean CAR: {percent(row.mean_car, 3)}</span>
      <span>p-value: {number(row.p_value, 4)}</span>
      <span>{row.significant ? "significant" : "not significant"}</span>
    </div>
  );
}

function ViralFeed({ events }) {
  const [openId, setOpenId] = useState(null);
  if (!events?.length) return <EmptyState text="No viral papers in the selected window." />;

  return (
    <div className="feed-list">
      {events.map((event) => {
        const open = openId === event.viral_event_id;
        return (
          <article className="feed-row" key={event.viral_event_id}>
            <button
              className="feed-main"
              type="button"
              onClick={() => setOpenId(open ? null : event.viral_event_id)}
            >
              <div>
                <h3>{event.title}</h3>
                <p>
                  {event.sector} | {compactDate(event.publication_date)} | CAS {number(event.cas, 2)}
                </p>
              </div>
              <div className="feed-metrics">
                <span>Reddit {integer(event.reddit_hits)}</span>
                <span>Wiki {integer(event.wiki_hits)}</span>
                <span>Velocity {number(event.cit_velocity, 2)}</span>
              </div>
            </button>

            {open && (
              <div className="feed-detail">
                <div className="detail-grid">
                  <div>
                    <span>Historical mean CAR+5</span>
                    <strong>{percent(event.historical_context?.mean_car_5d, 3)}</strong>
                  </div>
                  <div>
                    <span>Historical events</span>
                    <strong>{integer(event.historical_context?.n_events)}</strong>
                  </div>
                  <div>
                    <span>Detection date</span>
                    <strong>{compactDate(event.detected_date)}</strong>
                  </div>
                  <div>
                    <span>Source</span>
                    <strong>{event.source_display_name || "n/a"}</strong>
                  </div>
                </div>
                <div className="detail-actions">
                  {event.doi_url && (
                    <a href={event.doi_url} target="_blank" rel="noreferrer">
                      DOI <ExternalLink size={14} aria-hidden="true" />
                    </a>
                  )}
                </div>
                <div className="mini-window">
                  {(event.event_window || []).map((point) => (
                    <span key={`${event.viral_event_id}-${point.day_relative}`}>
                      d{point.day_relative}: {percent(point.car, 2)}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </article>
        );
      })}
    </div>
  );
}

function ResearchPage({ sector }) {
  const defaultTerms = useMemo(() => (sector?.keywords || []).join("\n"), [sector]);
  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const [keywords, setKeywords] = useState(defaultTerms);
  const [ticker, setTicker] = useState(sector?.weekly_ticker || "");
  const [dateStart, setDateStart] = useState("2019-01-01");
  const [dateEnd, setDateEnd] = useState(today);
  const [signal, setSignal] = useState("pub_zscore");
  const [run, setRun] = useState(null);
  const [error, setError] = useState(null);
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    setKeywords(defaultTerms);
    setTicker(sector?.weekly_ticker || "");
  }, [defaultTerms, sector?.weekly_ticker]);

  useEffect(() => {
    if (!run?.id || !["queued", "running"].includes(run.status)) return undefined;
    let cancelled = false;
    const poll = async () => {
      try {
        const data = await fetchResearchRun(run.id);
        if (!cancelled) setRun(data.run);
      } catch (pollError) {
        if (!cancelled) setError(pollError);
      }
    };
    const interval = window.setInterval(poll, 2500);
    poll();
    return () => {
      cancelled = true;
      window.clearInterval(interval);
    };
  }, [run?.id, run?.status]);

  async function handleSubmit(event) {
    event.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const data = await createResearchRun({
        keywords,
        ticker,
        date_start: dateStart,
        date_end: dateEnd,
      });
      setRun(data.run);
    } catch (submitError) {
      setError(submitError);
    } finally {
      setSubmitting(false);
    }
  }

  const progress = run?.progress || {};
  const result = run?.result;
  const selectedResult = result?.signals?.[signal];

  return (
    <div className="page-stack">
      <section className="research-layout">
        <Panel title="Custom Research Run" icon={FileSearch}>
          <form className="research-form" onSubmit={handleSubmit}>
            <label>
              Keywords
              <textarea
                value={keywords}
                onChange={(event) => setKeywords(event.target.value)}
                rows={7}
                placeholder={"large language model\nretrieval augmented generation"}
              />
            </label>
            <div className="form-grid">
              <label>
                Ticker
                <input
                  value={ticker}
                  onChange={(event) => setTicker(event.target.value.toUpperCase())}
                  placeholder="BOTZ"
                />
              </label>
              <label>
                Start
                <input
                  type="date"
                  min="2015-01-01"
                  max={dateEnd}
                  value={dateStart}
                  onChange={(event) => setDateStart(event.target.value)}
                />
              </label>
              <label>
                End
                <input
                  type="date"
                  min={dateStart}
                  max={today}
                  value={dateEnd}
                  onChange={(event) => setDateEnd(event.target.value)}
                />
              </label>
            </div>
            <button className="primary-action enabled" type="submit" disabled={submitting || run?.status === "running" || run?.status === "queued"}>
              <Search size={16} aria-hidden="true" />
              {submitting ? "Submitting" : "Run Analysis"}
            </button>
          </form>
        </Panel>
        <Panel title="Run State" icon={Database}>
          <dl className="update-list">
            <div>
              <dt>Status</dt>
              <dd>{run?.status || "Ready"}</dd>
            </div>
            <div>
              <dt>Progress</dt>
              <dd>{progress.pct !== undefined ? `${progress.pct}%` : "0%"}</dd>
            </div>
            <div>
              <dt>Stage</dt>
              <dd>{progress.message || "Configure inputs and run"}</dd>
            </div>
            <div>
              <dt>Run ID</dt>
              <dd>{run?.id ? run.id.slice(0, 8) : "n/a"}</dd>
            </div>
          </dl>
          {run && ["queued", "running"].includes(run.status) && (
            <div className="progress-track" aria-label="Research run progress">
              <span style={{ width: `${Math.max(3, progress.pct || 0)}%` }} />
            </div>
          )}
          {(error || run?.error_message) && (
            <div className="state-block error research-error">
              {error?.message || run.error_message}
            </div>
          )}
        </Panel>
      </section>

      {result && (
        <>
          <div className="analysis-controls">
            <div className="context-strip">
              Custom run for {result.ticker}: {result.keywords.join(", ")}
            </div>
            <label className="select-label compact">
              Signal
              <span className="select-wrap">
                <select value={signal} onChange={(event) => setSignal(event.target.value)}>
                  {SIGNALS.map((item) => (
                    <option key={item} value={item}>
                      {signalLabels[item]}
                    </option>
                  ))}
                </select>
                <ChevronDown size={16} aria-hidden="true" />
              </span>
            </label>
          </div>
          {selectedResult ? (
            <MomentumTab
              signal={signal}
              state={{
                loading: false,
                error: null,
                data: {
                  result: selectedResult,
                  series: result.series.map((row) => ({
                    ...row,
                    selected_signal: row[signal],
                  })),
                },
              }}
            />
          ) : (
            <EmptyState text="No result is available for the selected signal." />
          )}
        </>
      )}
    </div>
  );
}

function Panel({ title, icon: Icon, children }) {
  return (
    <section className="panel">
      <div className="panel-title">
        <Icon size={18} aria-hidden="true" />
        <h2>{title}</h2>
      </div>
      {children}
    </section>
  );
}

function MetricCard({ label, value, detail }) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{detail}</small>
    </div>
  );
}

function LoadingBlock() {
  return <div className="state-block">Loading seeded data...</div>;
}

function ErrorBlock({ error }) {
  return <div className="state-block error">{error.message}</div>;
}

function EmptyState({ text }) {
  return <div className="empty-state">{text}</div>;
}

function lagColor(row) {
  if (row.sig_bonf) return "#8f3f71";
  if (row.sig_05) return "#d18f3d";
  return "#8aa0a0";
}

function filterDisplayWeeks(rows) {
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  return rows.filter((row) => {
    const start = parseDate(row.week_start);
    if (!start) return true;
    const end = new Date(start);
    end.setDate(start.getDate() + 7);
    if (end > today) return false;
    return !weekContainsJan1(start);
  });
}

function parseDate(value) {
  if (!value) return null;
  const date = new Date(`${value.slice(0, 10)}T00:00:00`);
  return Number.isNaN(date.getTime()) ? null : date;
}

function weekContainsJan1(start) {
  const end = new Date(start);
  end.setDate(start.getDate() + 6);
  for (let year = start.getFullYear(); year <= end.getFullYear(); year += 1) {
    const jan1 = new Date(`${year}-01-01T00:00:00`);
    if (jan1 >= start && jan1 <= end) return true;
  }
  return false;
}

function normalizeCarRows(car) {
  if (!car) return [];
  if (Array.isArray(car)) {
    return car.map((row) => ({
      ...row,
      window: row.window || row.horizon || row.days || row.day || "CAR",
      mean_car: row.mean_car ?? row.mean ?? row.car,
    }));
  }
  return Object.entries(car)
    .filter(([, value]) => value && typeof value === "object")
    .map(([key, value]) => ({
      window: key,
      mean_car: value.mean_car ?? value.mean ?? value.car,
      ...value,
    }));
}
