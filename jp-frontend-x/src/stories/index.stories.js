import { addDecorator } from '@storybook/react';
import React, { Suspense } from 'react';
import { BrowserRouter } from 'react-router-dom';
import { GlobalStyles } from '../design-system/theme';
import { ThemeProvider } from 'emotion-theming';

import '../i18n';
import { MaxPanel, Text, theme } from '../design-system';

addDecorator((story) => (
  <Suspense
    fallback={
      <MaxPanel>
        <Text>loading...</Text>
      </MaxPanel>
    }
  >
    <ThemeProvider theme={theme}>
      <GlobalStyles />
      <BrowserRouter>{story()}</BrowserRouter>
    </ThemeProvider>
  </Suspense>
));
