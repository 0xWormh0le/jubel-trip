import 'normalize.css';

import { Global, css } from '@emotion/core';

import React from 'react';

/**
 * This is the global style sheet...it should not get too full of stuff that could better
 * be done in components...but it can do some useful common things.
 */
const GlobalStyles = () => (
  <Global
    styles={(theme) => css`

    @import url('https://fonts.googleapis.com/css?family=${
      theme.headingsFamily
    }:400,700|${theme.fontfamily}:400,700');

    * {
     box-sizing: border-box;
     border-color: ${theme.colors.border};
    }

    html {
      font-size: ${theme.htmlFontScale}%;
    }

    html,
    body,
    #root {
      margin: 0;
      padding: 0;
      height: 100%;
      overflow: hidden;
    }

    body {
      font-family: ${theme.fontFamily};
      font-size: ${theme.fontSizes[5]};
      color: ${theme.colors.on.background};
      background: ${theme.colors.background};
    }

    h1,
    h2,
    h3,
    h4,
    h5,
    h6,
    .heading {
      margin: 0;
      padding: 0;
      font-family: ${theme.headingsFamily};
      font-weight: bold;
    }

    h1 {
      font-size: ${theme.fontSizes[10]};
    }

    h2 {
      font-size: ${theme.fontSizes[9]};
    }

    h3 {
      font-size: ${theme.fontSizes[8]};
    }

    h4 {
      font-size: ${theme.fontSizes[7]};
    }

    h5 {
      font-size: ${theme.fontSizes[6]};
    }

    h6 {
      font-size: ${theme.fontSizes[5]};
    }

    .surface {
      color: ${theme.colors.on.surface};
      background: ${theme.colors.surface};
    }

    a:link,
    a:visited,
    a:active {
      text-decoration: none;
      cursor: pointer;
      color: ${theme.colors.primary};
      background: transparent;
    }

    button, .hoverable {
      cursor: pointer;
    }

    a:disabled, button:disabled, .hoverable:disabled, .disabled {
      cursor: default;
    }

    :active {
      // box-shadow: none!important;
    }

    :focus {
      outline: none;
    }
  
    .focus-visible {
      outline: 0.125rem solid ${theme.colors.misc.outline};
    }

    input, button, textarea {
      transition: box-shadow 0.2s ease-in-out;
    }

    input.focus-visible, button.focus-visible, textarea.focus-visible {
      outline: none;
      box-shadow: 0 0 0 0.125rem ${theme.colors.misc.outline};
    }

    .remove-input-focus input.focus-visible,
    .remove-button-focus button.focus-visible,
    .remove-textarea-focus textarea.focus-visible {
      box-shadow: none;
    }

    button {
      padding: 0;
    }

    .ripple {
      border-radius:50%;
      top: 50%;
      left: 50%;
      opacity: 0.5;

      -webkit-transition: height .25s ease, width .25s ease;
      transition: height .25s ease, width .25s ease;
      
      -webkit-transform: translate(-50%,-50%);
      transform: translate(-50%,-50%);
    }

    .ripple:before,
    .ripple:after {
      content:'';
      display:block;
      position:absolute;
      top: 0;
      right: 0;
      bottom: 0;
      left: 0;
      border-radius: 50%;
      border: 4px solid ${theme.colors.primary};
    }

    .ripple:before {
      -webkit-animation: ripple 4s linear infinite;
      animation: ripple 4s linear infinite;
    }

    .ripple:after {
      -webkit-animation: ripple 4s linear 2s infinite;
      animation: ripple 4s linear 2s infinite;
    }

    @-webkit-keyframes ripple {
      0% { -webkit-transform: scale(1); }
      25% { -webkit-transform: scale(1.5); opacity: 1; }
      50% { -webkit-transform: scale(2); opacity: 0; }
      51% { -webkit-transform: scale(0); opacity: 0; }
      100% { -webkit-transform: scale(1); }
    }

    @keyframes ripple {
      0% { transform: scale(1); }
      25% { transform: scale(1.5); opacity: 1; }
      50% { transform: scale(2); opacity: 0; }
      51% { transform: scale(0); opacity: 0; }
      100% { transform: scale(1); }
    }

    .DayPicker {
      font-size: .8rem;
    } 

    .DayPicker-Day--selected.DayPicker-Day--outside,
    .DayPicker-Day--departureDate.DayPicker-Day--outside,
    .DayPicker-Day--returnDate.DayPicker-Day--outside {
      background: ${theme.colors.misc.datePicker.rangeOutsideDays.bg}!important;
      color: ${theme.colors.misc.datePicker.rangeOutsideDays.color}!important;
    }

    .main-search::placeholder {
      text-align: center;
    }
    .travel-map-marker {
      background-image: url(data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHdpZHRoPSIyNCIgaGVpZ2h0PSIyNCIgdmlld0JveD0iMCAwIDI0IDI0Ij4gICAgPHBhdGggZmlsbD0iI2Y2NjE2MSIgZD0iTTEyIDJDOC4xMyAyIDUgNS4xMyA1IDljMCA1LjI1IDcgMTMgNyAxM3M3LTcuNzUgNy0xM2MwLTMuODctMy4xMy03LTctN3ptMCA5LjVjLTEuMzggMC0yLjUtMS4xMi0yLjUtMi41czEuMTItMi41IDIuNS0yLjUgMi41IDEuMTIgMi41IDIuNS0xLjEyIDIuNS0yLjUgMi41eiIvPjwvc3ZnPg==);
      background-size: cover;
      width: 32px;
      height: 32px;
      border-radius: 50%;
      cursor: pointer;
    }

    @-webkit-keyframes spin {
      0% {
        -webkit-transform: rotate(0deg);
                transform: rotate(0deg);
      }
      100% {
        -webkit-transform: rotate(359deg);
                transform: rotate(359deg);
      }
    }
    @keyframes spin {
      from {transform:rotate(0deg);}
      to {transform:rotate(360deg);}
    }

    .react-hint--top:after {
      border-top-color: ${theme.colors.on.surface};
    }

    .react-hint--left:after {
      border-left-color: ${theme.colors.on.surface};
    }

    .react-hint--right:after {
      border-right-color: ${theme.colors.on.surface};
    }

    .react-hint--bottom:after {
      border-bottom-color: ${theme.colors.on.surface};
    }
  `}
  />
);

export default GlobalStyles;
