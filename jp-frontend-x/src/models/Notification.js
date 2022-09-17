import { observable, action, decorate } from 'mobx';

class Notification {
  date;
  message;
  seen = false;

  constructor(props) {
    ['date', 'message', 'seen'].forEach((p) => {
      if (props.hasOwnProperty(p)) this[p] = props[p];
    });
  }

  markSeen() {
    this.seen = true;
  }

  markUnseen() {
    this.seen = false;
  }
}

export default Notification;

decorate(Notification, {
  seen: observable,
  markSeen: action,
  markUnseen: action,
});
