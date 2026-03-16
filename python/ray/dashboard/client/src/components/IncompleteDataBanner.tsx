import { Alert, AlertTitle, Box } from "@mui/material";
import React from "react";

export const IncompleteDataBanner = () => {
  return (
    <Box
      sx={{
        width: "100%",
        position: "fixed",
        left: 0,
        right: 0,
        bottom: 0,
        zIndex: 999,
      }}
    >
      <Alert severity="warning" sx={{ borderRadius: 0 }}>
        <AlertTitle>Incomplete Data</AlertTitle>
        The data for this workload may be incomplete. This can happen if the
        cluster terminated abnormally or if data collection errored during
        execution.
      </Alert>
    </Box>
  );
};
