import { Alert, AlertTitle, Box } from "@mui/material";
import React, { useContext } from "react";
import { MainNavContext } from "../pages/layout/mainNavContext";

const MAIN_NAV_HEIGHT = 56;
const BREADCRUMBS_HEIGHT = 36;

export const IncompleteDataBanner = () => {
  const { mainNavPageHierarchy } = useContext(MainNavContext);
  const tallNav = mainNavPageHierarchy.length > 1;
  
  const topOffset = tallNav 
    ? MAIN_NAV_HEIGHT + BREADCRUMBS_HEIGHT + 2 // +2 for border
    : MAIN_NAV_HEIGHT;

  return (
    <Box
      sx={{
        width: "100%",
        position: "fixed",
        top: `${topOffset}px`,
        left: 0,
        right: 0,
        zIndex: 999,
      }}
    >
      <Alert severity="warning" sx={{ borderRadius: 0 }}>
        <AlertTitle>Incomplete Data</AlertTitle>
        The data for this workload may be incomplete. This can happen if the cluster terminated abnormally or if data collection errored during execution.
      </Alert>
    </Box>
  );
};
