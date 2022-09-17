import { travelStyles } from './TravelStyle';
import { action, observable, decorate } from 'mobx';
import theme from '../design-system/theme';

const getColor = (style, key) => theme.colors.misc.branding.styles[style][key];
const normalize = (values) => {
  const sum = Object.values(values).reduce((s, v) => s + v, 0);
  return Object.keys(values).reduce((o, key) => {
    const v = values[key];
    if (v > 0) {
      o[key] = (100 * v) / sum;
    } else {
      o[key] = v;
    }
    return o;
  }, {});
};

export default class TravelDistribution {
  rawData = travelStyles.map((ts) => ({
    x: ts.abbr,
    y: 0,
    name: ts.name,
    onColor: getColor(ts.abbr, 'text'),
    color: getColor(ts.abbr, 'bg'),
  }));

  rawDataObject = {};

  constructor(values) {
    this.setData(values);
  }

  setData(values) {
    this.rawDataObject = values;
    const normalized = normalize(values);
    let dataCopy = this.rawData;
    Object.keys(normalize(values)).forEach((key) => {
      const value = normalized[key];
      const i = dataCopy.findIndex((d) => d.x === key);
      if (i >= 0) dataCopy[i].y = value;
    });
    this.rawData = dataCopy;
  }

  getData(includeZeroes = false) {
    return this.rawData.filter((d) => includeZeroes || d.y > 0);
  }

  getDataObject() {
    return this.rawDataObject;
  }

  getColors(includeZeroes = false) {
    return this.getData(includeZeroes).map(
      (_) => _.color || getColor(_.abbr, 'bg')
    );
  }

  async updateChartValues({ name, values }) {
    const dataObject = this.getDataObject();
    if (dataObject[name] !== values[0]) {
      dataObject[name] = values[0];
      await this.setData(dataObject);
    }
  }
}

decorate(TravelDistribution, {
  rawData: observable,
  rawDataObject: observable,
  setData: action,
  updateChartValues: action,
});
