import React from 'react';
import { mount, shallow } from 'enzyme';
import { ThemeProvider } from 'emotion-theming';
import theme from '../design-system/theme';

export const mountWithTheme = (component) =>
  mount(<ThemeProvider theme={theme}>{component}</ThemeProvider>);

export const shallowWithTheme = (component) =>
  shallow(<ThemeProvider theme={theme}>{component}</ThemeProvider>);
