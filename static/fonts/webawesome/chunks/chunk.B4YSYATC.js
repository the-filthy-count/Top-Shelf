/*! Copyright 2026 Fonticons, Inc. - https://webawesome.com/license */
import {
  spinner_styles_default
} from "./chunk.W7A2VLCT.js";
import {
  LocalizeController
} from "./chunk.JHBJNH5V.js";
import {
  WebAwesomeElement
} from "./chunk.QX3YGD4G.js";
import {
  __decorateClass
} from "./chunk.7VGCIHDG.js";

// src/components/spinner/spinner.ts
import { html } from "lit";
import { customElement } from "lit/decorators.js";
var WaSpinner = class extends WebAwesomeElement {
  constructor() {
    super(...arguments);
    this.localize = new LocalizeController(this);
  }
  render() {
    return html`
      <svg
        part="base"
        role="progressbar"
        aria-label=${this.localize.term("loading")}
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
      >
        <circle class="track" />
        <circle class="indicator" />
      </svg>
    `;
  }
};
WaSpinner.css = spinner_styles_default;
WaSpinner = __decorateClass([
  customElement("wa-spinner")
], WaSpinner);

export {
  WaSpinner
};
