import React from 'react';
import { inject, observer } from 'mobx-react';
import compose from 'lodash/fp/compose';

import { MaxPanel } from '../design-system';

class Screen extends React.Component {
  componentDidMount = () => {
    const { trackScreenView, name = 'unknown' } = this.props;
    trackScreenView(name);
  };

  render() {
    const { children, full = false, ...props } = this.props;
    return full ? (
      <MaxPanel
        position="absolute"
        top={0}
        left={0}
        bottom={0}
        right={0}
        {...props}
      >
        {children}
      </MaxPanel>
    ) : (
      <>{children}</>
    );
  }
}

Screen.displayname = 'Screen';

Screen.defaultProps = {
  name: 'screen',
};

export default compose(
  inject(({ app }) => ({
    trackScreenView: app.trackScreenView,
  })),
  observer
)(Screen);
