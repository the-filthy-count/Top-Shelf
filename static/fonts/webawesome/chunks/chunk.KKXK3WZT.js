/*! Copyright 2026 Fonticons, Inc. - https://webawesome.com/license */
import {
  WaIcon
} from "./chunk.SFADCYQ3.js";

// src/react/icon/index.ts
import { createComponent } from "@lit/react";
import * as React from "react";
import "@lit/react";
var tagName = "wa-icon";
var reactWrapper = createComponent({
  tagName,
  elementClass: WaIcon,
  react: React,
  events: {
    onWaLoad: "wa-load",
    onWaError: "wa-error"
  },
  displayName: "WaIcon"
});
var icon_default = reactWrapper;

export {
  icon_default
};
