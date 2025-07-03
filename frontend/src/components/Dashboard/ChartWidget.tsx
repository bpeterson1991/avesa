import React from 'react';
import { BaseWidget } from './BaseWidget';
import { WidgetProps } from '../../types/widget';
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts';

const COLORS = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#06B6D4', '#EC4899', '#F97316'];

interface ChartWidgetOptions {
  xAxisKey?: string;
  yAxisKey?: string;
  dataKeys?: string[];
  colors?: string[];
  showLegend?: boolean;
  showGrid?: boolean;
  height?: number;
}

export const ChartWidget: React.FC<WidgetProps> = (props) => {
  const { config, data } = props;
  const chartData = data.data as any[];
  const options = config.visualization.options as ChartWidgetOptions;

  const renderChart = () => {
    if (!chartData || !Array.isArray(chartData) || chartData.length === 0) {
      return (
        <div className="flex items-center justify-center h-full text-gray-500">
          No data available
        </div>
      );
    }

    const chartColors = options.colors || COLORS;
    const height = options.height || 300;

    switch (config.visualization.type) {
      case 'bar':
        return (
          <ResponsiveContainer width="100%" height={height}>
            <BarChart data={chartData}>
              {options.showGrid !== false && <CartesianGrid strokeDasharray="3 3" />}
              <XAxis 
                dataKey={options.xAxisKey || 'name'} 
                fontSize={12}
                angle={-45}
                textAnchor="end"
                height={80}
              />
              <YAxis fontSize={12} />
              <Tooltip />
              {options.showLegend && <Legend />}
              {(options.dataKeys || ['value']).map((key, index) => (
                <Bar 
                  key={key} 
                  dataKey={key} 
                  fill={chartColors[index % chartColors.length]} 
                />
              ))}
            </BarChart>
          </ResponsiveContainer>
        );

      case 'line':
        return (
          <ResponsiveContainer width="100%" height={height}>
            <LineChart data={chartData}>
              {options.showGrid !== false && <CartesianGrid strokeDasharray="3 3" />}
              <XAxis 
                dataKey={options.xAxisKey || 'date'} 
                fontSize={12}
              />
              <YAxis fontSize={12} />
              <Tooltip />
              {options.showLegend && <Legend />}
              {(options.dataKeys || ['value']).map((key, index) => (
                <Line 
                  key={key} 
                  type="monotone" 
                  dataKey={key} 
                  stroke={chartColors[index % chartColors.length]}
                  strokeWidth={2}
                />
              ))}
            </LineChart>
          </ResponsiveContainer>
        );

      case 'pie':
        return (
          <ResponsiveContainer width="100%" height={height}>
            <PieChart>
              <Pie
                data={chartData}
                cx="50%"
                cy="50%"
                labelLine={false}
                label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}
                outerRadius={80}
                fill="#8884d8"
                dataKey={options.yAxisKey || 'value'}
              >
                {chartData.map((entry, index) => (
                  <Cell key={`cell-${index}`} fill={chartColors[index % chartColors.length]} />
                ))}
              </Pie>
              <Tooltip />
            </PieChart>
          </ResponsiveContainer>
        );

      default:
        return (
          <div className="flex items-center justify-center h-full text-gray-500">
            Unsupported chart type: {config.visualization.type}
          </div>
        );
    }
  };

  return (
    <BaseWidget {...props}>
      {renderChart()}
    </BaseWidget>
  );
};