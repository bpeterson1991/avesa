import React from 'react';
import { BaseWidget } from './BaseWidget';
import { WidgetProps } from '../../types/widget';
import { ArrowTrendingUpIcon, ArrowTrendingDownIcon } from '@heroicons/react/24/outline';

interface KPIData {
  value: number | string;
  label?: string;
  change?: number;
  changeType?: 'increase' | 'decrease' | 'neutral';
  unit?: string;
  format?: 'number' | 'currency' | 'percentage';
  icon?: React.ComponentType<any>;
  color?: string;
}

export const KPIWidget: React.FC<WidgetProps> = (props) => {
  const { data } = props;
  const kpiData = data.data as KPIData;

  const formatValue = (value: number | string, format?: string, unit?: string): string => {
    if (typeof value === 'string') return value;
    
    let formatted = '';
    switch (format) {
      case 'currency':
        formatted = new Intl.NumberFormat('en-US', {
          style: 'currency',
          currency: 'USD',
          minimumFractionDigits: 0,
          maximumFractionDigits: 0
        }).format(value);
        break;
      case 'percentage':
        formatted = `${value}%`;
        break;
      default:
        formatted = value.toLocaleString();
    }
    
    return unit && format !== 'currency' ? `${formatted} ${unit}` : formatted;
  };

  const getChangeIcon = (changeType?: string) => {
    if (changeType === 'increase') {
      return <ArrowTrendingUpIcon className="h-4 w-4 text-green-500" />;
    } else if (changeType === 'decrease') {
      return <ArrowTrendingDownIcon className="h-4 w-4 text-red-500" />;
    }
    return null;
  };

  const getChangeColor = (changeType?: string) => {
    if (changeType === 'increase') return 'text-green-600';
    if (changeType === 'decrease') return 'text-red-600';
    return 'text-gray-600';
  };

  return (
    <BaseWidget {...props}>
      {kpiData && (
        <div className="flex flex-col items-center justify-center h-full">
          {/* Icon */}
          {kpiData.icon && (
            <div className={`w-12 h-12 ${kpiData.color || 'bg-blue-100'} rounded-lg flex items-center justify-center mb-3`}>
              <kpiData.icon className={`w-6 h-6 ${kpiData.color ? 'text-white' : 'text-blue-600'}`} />
            </div>
          )}
          
          {/* Main Value */}
          <div className="text-center">
            <div className="text-3xl font-bold text-gray-900">
              {formatValue(kpiData.value, kpiData.format, kpiData.unit)}
            </div>
            
            {/* Label */}
            {kpiData.label && (
              <div className="text-sm text-gray-500 mt-1">
                {kpiData.label}
              </div>
            )}
            
            {/* Change Indicator */}
            {kpiData.change !== undefined && (
              <div className={`flex items-center justify-center mt-2 text-sm font-medium ${getChangeColor(kpiData.changeType)}`}>
                {getChangeIcon(kpiData.changeType)}
                <span className="ml-1">
                  {kpiData.change > 0 ? '+' : ''}{kpiData.change}%
                </span>
              </div>
            )}
          </div>
        </div>
      )}
    </BaseWidget>
  );
};