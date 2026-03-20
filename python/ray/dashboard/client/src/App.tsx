import { CssBaseline } from "@mui/material";
import { StyledEngineProvider, ThemeProvider } from "@mui/material/styles";
import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";
import React, { Suspense, useEffect, useState } from "react";
import { HashRouter, Navigate, Route, Routes } from "react-router-dom";
import ActorDetailPage, { ActorDetailLayout } from "./pages/actor/ActorDetail";
import { ActorLayout } from "./pages/actor/ActorLayout";
import Loading from "./pages/exception/Loading";
import JobList, { JobsLayout } from "./pages/job";
import { JobDetailChartsPage } from "./pages/job/JobDetail";
import {
  JobDetailActorDetailWrapper,
  JobDetailActorsPage,
} from "./pages/job/JobDetailActorPage";
import { JobDetailInfoPage } from "./pages/job/JobDetailInfoPage";
import { JobDetailLayout, JobPage } from "./pages/job/JobDetailLayout";
import { MainNavLayout } from "./pages/layout/MainNavLayout";
import { SideTabPage } from "./pages/layout/SideTabLayout";
import {
  LogsLayout,
  StateApiLogsListPage,
  StateApiLogViewerPage,
} from "./pages/log/Logs";
import { Metrics } from "./pages/metrics";
import {
  DashboardUids,
  getMetricsInfo,
  getTimeZoneInfo,
  TimezoneInfo,
} from "./pages/metrics/utils";
import Nodes, { ClusterMainPageLayout } from "./pages/node";
import { ClusterDetailInfoPage } from "./pages/node/ClusterDetailInfoPage";
import { ClusterLayout } from "./pages/node/ClusterLayout";
import NodeDetailPage from "./pages/node/NodeDetail";
import { OverviewPage } from "./pages/overview/OverviewPage";
import { TaskPage } from "./pages/task/TaskPage";
import { getNodeList } from "./service/node";
import { lightTheme } from "./theme";

dayjs.extend(duration);

// lazy loading fro prevent loading too much code at once
const Actors = React.lazy(() => import("./pages/actor"));
const CMDResult = React.lazy(() => import("./pages/cmd/CMDResult"));

// a global map for relations
export type GlobalContextType = {
  nodeMap: { [key: string]: string };
  nodeMapByIp: { [key: string]: string };
  namespaceMap: { [key: string]: string[] };
  /**
   * Whether the initial metrics context has been fetched or not.
   * This can be used to determine the difference between Grafana
   * not being set up vs the status not being fetched yet.
   */
  metricsContextLoaded: boolean;
  /**
   * The host that is serving grafana. Only set if grafana is
   * running as detected by the grafana healthcheck endpoint.
   */
  grafanaHost: string | undefined;
  /**
   * The param 'orgId' used in grafana. Default is 1.
   */
  grafanaOrgId: string;
  /**
   * The filter for the Cluster variable in grafana dashboards.
   */
  grafanaClusterFilter: string | undefined;
  /**
   * The uids of the dashboards that ray exports that powers the various metrics UIs.
   */
  dashboardUids: DashboardUids | undefined;
  /**
   * Whether prometheus is runing or not
   */
  prometheusHealth: boolean | undefined;
  /**
   * The name of the currently running ray session.
   */
  sessionName: string | undefined;
  /**
   * The name of the current selected datasource.
   */
  dashboardDatasource: string | undefined;
  /**
   * The timezone set on the ray cluster.
   */
  serverTimeZone: TimezoneInfo | null | undefined;
  /**
   * The globally selected current time zone.
   */
  currentTimeZone: string | undefined;
  /**
   * Default start of the Grafana time range. When set, overrides the
   * default "now-5m" value.
   */
  grafanaDefaultFrom: string | undefined;
  /**
   * Default end of the Grafana time range.
   */
  grafanaDefaultTo: string | undefined;
  /**
   * Whether the data for this workload is complete or not.
   * For live clusters, this is always true.
   */
  dataComplete: boolean;
  /**
   * Whether we're viewing a historical dashboard (history server)
   * vs a live cluster dashboard.
   */
  isHistoricalDashboard: boolean;
};
export const GlobalContext = React.createContext<GlobalContextType>({
  nodeMap: {},
  nodeMapByIp: {},
  namespaceMap: {},
  metricsContextLoaded: false,
  grafanaHost: undefined,
  grafanaOrgId: "1",
  grafanaClusterFilter: undefined,
  dashboardUids: undefined,
  prometheusHealth: undefined,
  sessionName: undefined,
  dashboardDatasource: undefined,
  grafanaDefaultFrom: undefined,
  grafanaDefaultTo: undefined,
  serverTimeZone: undefined,
  currentTimeZone: undefined,
  dataComplete: true,
  isHistoricalDashboard: false,
});

const App = () => {
  const [currentTimeZone, setCurrentTimeZone] = useState<string>();
  const [context, setContext] = useState<
    Omit<GlobalContextType, "currentTimeZone">
  >({
    nodeMap: {},
    nodeMapByIp: {},
    namespaceMap: {},
    metricsContextLoaded: false,
    grafanaHost: undefined,
    grafanaOrgId: "1",
    grafanaClusterFilter: undefined,
    dashboardUids: undefined,
    prometheusHealth: undefined,
    sessionName: undefined,
    dashboardDatasource: undefined,
    grafanaDefaultFrom: undefined,
    grafanaDefaultTo: undefined,
    serverTimeZone: undefined,
    dataComplete: true,
    isHistoricalDashboard: false,
  });
  useEffect(() => {
    getNodeList().then((res) => {
      if (res?.data?.data?.summary) {
        const nodeMap = {} as { [key: string]: string };
        const nodeMapByIp = {} as { [key: string]: string };
        res.data.data.summary.forEach(({ hostname, raylet, ip }) => {
          nodeMap[hostname] = raylet.nodeId;
          nodeMapByIp[ip] = raylet.nodeId;
        });
        setContext((existingContext) => ({
          ...existingContext,
          nodeMap,
          nodeMapByIp,
          namespaceMap: {},
        }));
      }
    });
  }, []);

  // Detect if grafana is running
  useEffect(() => {
    const doEffect = async () => {
      const {
        grafanaHost,
        grafanaOrgId,
        grafanaClusterFilter,
        sessionName,
        prometheusHealth,
        dashboardUids,
        dashboardDatasource,
        grafanaDefaultFrom,
        grafanaDefaultTo,
      } = await getMetricsInfo();
      setContext((existingContext) => ({
        ...existingContext,
        metricsContextLoaded: true,
        grafanaHost,
        grafanaOrgId,
        grafanaClusterFilter,
        dashboardUids,
        sessionName,
        prometheusHealth,
        dashboardDatasource,
        grafanaDefaultFrom,
        grafanaDefaultTo,
      }));
    };
    doEffect();
  }, []);

  useEffect(() => {
    const updateTimezone = async () => {
      // Sets the initial timezone to localStorage value if it exists
      const storedTimeZone = localStorage.getItem("timezone");
      if (storedTimeZone) {
        setCurrentTimeZone(storedTimeZone);
      }

      // Fetch the server time zone.
      const tzInfo = await getTimeZoneInfo();

      const timeZone =
        storedTimeZone ||
        tzInfo?.value ||
        Intl.DateTimeFormat().resolvedOptions().timeZone;

      setCurrentTimeZone(timeZone);
      setContext((existingContext) => ({
        ...existingContext,
        serverTimeZone: tzInfo,
      }));
    };
    updateTimezone();
  }, []);

  useEffect(() => {
    const checkDataCompleteness = async () => {
      const isHistorical = window.location.pathname.includes('/lilypad/history/');
      
      if (!isHistorical) {
        // Live cluster- data is always complete
        setContext((existingContext) => ({
          ...existingContext,
          isHistoricalDashboard: false,
          dataComplete: true,
        }));
        return;
      }
      
      try {
        // Construct absolute path to status endpoint: /lilypad/history/{uuid}/status
        const pathMatch = window.location.pathname.match(/\/lilypad\/history\/[^/]+/);
        if (!pathMatch) {
          throw new Error("Could not parse history UUID from pathname");
        }
        const statusUrl = `${pathMatch[0]}/status`;
        const response = await fetch(statusUrl);
        if (!response.ok) {
          throw new Error(`Status endpoint returned ${response.status}`);
        }
        const data = await response.json();

        setContext((existingContext) => ({
          ...existingContext,
          isHistoricalDashboard: true,
          dataComplete: data.data_complete !== false,
        }));
      } catch (error) {
        console.warn("Data completeness check failed, assuming incomplete:", error);
        setContext((existingContext) => ({
          ...existingContext,
          isHistoricalDashboard: true,
          dataComplete: false,
        }));
      }
    };
    checkDataCompleteness();
  }, []);

  return (
    <StyledEngineProvider injectFirst>
      <ThemeProvider theme={lightTheme}>
        <Suspense fallback={Loading}>
          <GlobalContext.Provider value={{ ...context, currentTimeZone }}>
            <CssBaseline />
            <HashRouter>
              <Routes>
                {/* Redirect people hitting the /new path to root. TODO(aguo): Delete this redirect in ray 2.5 */}
                <Route element={<Navigate replace to="/" />} path="/new" />
                <Route element={<MainNavLayout />} path="/">
                  <Route element={<Navigate replace to="overview" />} path="" />
                  <Route element={<OverviewPage />} path="overview" />
                  <Route element={<ClusterMainPageLayout />} path="cluster">
                    <Route element={<ClusterLayout />} path="">
                      <Route
                        element={
                          <SideTabPage tabId="info">
                            <ClusterDetailInfoPage />
                          </SideTabPage>
                        }
                        path="info"
                      />
                      <Route
                        element={
                          <SideTabPage tabId="table">
                            <Nodes />
                          </SideTabPage>
                        }
                        path=""
                      />
                    </Route>
                    <Route element={<NodeDetailPage />} path="nodes/:id" />
                  </Route>
                  <Route element={<JobsLayout />} path="jobs">
                    <Route element={<JobList />} path="" />
                    <Route element={<JobPage />} path=":id">
                      <Route element={<JobDetailLayout />} path="">
                        <Route
                          element={
                            <SideTabPage tabId="info">
                              <JobDetailInfoPage />
                            </SideTabPage>
                          }
                          path="info"
                        />
                        <Route
                          element={
                            <SideTabPage tabId="charts">
                              <JobDetailChartsPage />
                            </SideTabPage>
                          }
                          path=""
                        />
                        <Route
                          element={
                            <SideTabPage tabId="actors">
                              <JobDetailActorsPage />
                            </SideTabPage>
                          }
                          path="actors"
                        />
                      </Route>
                      <Route
                        element={
                          <JobDetailActorDetailWrapper>
                            <ActorDetailLayout />
                          </JobDetailActorDetailWrapper>
                        }
                        path="actors/:actorId"
                      >
                        <Route element={<ActorDetailPage />} path="" />
                        <Route element={<TaskPage />} path="tasks/:taskId" />
                      </Route>
                      <Route element={<TaskPage />} path="tasks/:taskId" />
                    </Route>
                  </Route>
                  <Route element={<ActorLayout />} path="actors">
                    <Route element={<Actors />} path="" />
                    <Route element={<ActorDetailLayout />} path=":actorId">
                      <Route element={<ActorDetailPage />} path="" />
                      <Route element={<TaskPage />} path="tasks/:taskId" />
                    </Route>
                  </Route>
                  <Route element={<Metrics />} path="metrics" />
                  <Route element={<LogsLayout />} path="logs">
                    <Route element={<StateApiLogsListPage />} path="" />
                    <Route element={<StateApiLogViewerPage />} path="viewer" />
                  </Route>
                </Route>
                <Route element={<CMDResult />} path="/cmd/:cmd/:ip/:pid" />
              </Routes>
            </HashRouter>
          </GlobalContext.Provider>
        </Suspense>
      </ThemeProvider>
    </StyledEngineProvider>
  );
};

export default App;
