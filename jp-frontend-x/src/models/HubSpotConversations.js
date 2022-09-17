// Proxy which checks if window.HubSpotConversations is loaded and performs actions specified in docs accordingly before each relevant method call, with exception of load method
const ChatWidgetProxy = (widget) => {
  return new Proxy(widget, {
    get(target, prop) {
      if (typeof target[prop] === 'function' && prop !== 'load') {
        if (window.HubSpotConversations) {
          return target[prop];
        } else {
          window.hsConversationsOnReady = [
            ...(window.hsConversationsOnReady || []),
            target[prop],
          ];
        }
      } else if (
        (prop === 'load' || prop === 'open') &&
        !window.HubSpotConversations.widget.status().loaded
      ) {
        console.error('Widget already loaded.');
        return target[prop];
      } else if (
        (prop === 'remove' || prop === 'close') &&
        !window.HubSpotConversations.widget.status().loaded
      ) {
        console.error('Widget is not present on the page.');
        return target[prop];
      } else {
        return target[prop];
      }
    },
  });
};

const ChatWidget = ({ children }) => {
  function _load() {
    window.HubSpotConversations.widget.load();
  }
  if (!window.HubSpotConversations) {
    window.hsConversationsOnReady = [_load];
  } else {
    window.HubSpotConversations.widget.load();
  }

  if (!children) {
    return null;
  } else {
    return children(ChatWidgetProxy(window.HubSpotConversations.widget));
  }
};

export default ChatWidget;
