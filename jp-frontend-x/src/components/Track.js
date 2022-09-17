import { inject, observer } from 'mobx-react';
import React from 'react';

class Track extends React.Component {
  constructor(props) {
    super(props);
    this.handleClick = this.handleClick.bind(this);
  }

  handleClick(evt) {
    const {
      app,
      event = 'event',
      category = 'links',
      action = 'click',
      label = evt.currentTarget &&
        (evt.currentTarget.title || evt.currentTarget.textContent),
      value,
      track = {},
    } = this.props;

    const data = Object.assign(
      { event, category, label, action, value },
      track || {}
    );

    app.trackEvent(data);
  }
  render() {
    const { children } = this.props;
    return (
      <div onClickCapture={(event) => this.handleClick(event)}>{children}</div>
    );
  }
}

export default inject('app')(observer(Track));
