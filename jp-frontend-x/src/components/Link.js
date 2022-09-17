import { inject, observer } from 'mobx-react';
import React from 'react';
import { Link as RRLink } from 'react-router-dom';

class Link extends React.Component {
  constructor(props) {
    super(props);
    this.handleClick = this.handleClick.bind(this);
  }

  handleClick(evt) {
    const {
      app,
      track = {},
      activatePathOnly,
      togglePath,
      to,
      active,
      closeMenu,
      event = 'event',
      category = 'links',
      label = evt.currentTarget &&
        (evt.currentTarget.title || evt.currentTarget.textContent),
      action = 'click',
      value,
    } = this.props;

    const data = Object.assign(
      { event, category, label, action, value },
      track || {}
    );
    app.trackEvent(data);
    if (togglePath) {
      const path = typeof togglePath === 'boolean' ? to : togglePath;
      if (path) app.togglePath(path);
      evt.preventDefault();
    } else if (!active) {
      if (activatePathOnly || to) {
        app.activatePathOnly(activatePathOnly || to);
      }
    } else evt.preventDefault();
    if (closeMenu) app.closeMenu();
  }

  render() {
    const {
      app,
      track,
      event,
      category,
      label,
      action,
      value,
      isActive,
      children,
      activatePathOnly,
      togglePath,
      closeMenu,
      ...props
    } = this.props;
    if (props.href || !props.to)
      return (
        <a {...props} onClickCapture={this.handleClick}>
          {children}
        </a>
      );
    else
      return (
        <RRLink {...props} onClickCapture={this.handleClick}>
          {children}
        </RRLink>
      );
  }
}

export default inject('app')(observer(Link));
