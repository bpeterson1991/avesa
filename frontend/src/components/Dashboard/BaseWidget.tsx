import React, { ReactNode } from 'react';
import { XMarkIcon, PencilIcon } from '@heroicons/react/24/outline';
import { WidgetProps } from '../../types/widget';

interface BaseWidgetProps extends WidgetProps {
  children: ReactNode;
  className?: string;
}

export const BaseWidget: React.FC<BaseWidgetProps> = ({
  config,
  data,
  onEdit,
  onRemove,
  isEditMode,
  children,
  className = ''
}) => {
  const handleEdit = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (onEdit) {
      onEdit(config);
    }
  };

  const handleRemove = (e: React.MouseEvent) => {
    e.stopPropagation();
    if (onRemove) {
      onRemove(config.id);
    }
  };

  return (
    <div className={`h-full bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden ${className}`}>
      {/* Widget Header */}
      <div className="px-4 py-3 border-b border-gray-200 flex items-center justify-between">
        <h3 className="text-sm font-medium text-gray-900 truncate">{config.title}</h3>
        {isEditMode && (
          <div className="flex items-center space-x-1">
            <button
              onClick={handleEdit}
              className="p-1 rounded hover:bg-gray-100 transition-colors"
              title="Edit widget"
            >
              <PencilIcon className="h-4 w-4 text-gray-500" />
            </button>
            <button
              onClick={handleRemove}
              className="p-1 rounded hover:bg-gray-100 transition-colors"
              title="Remove widget"
            >
              <XMarkIcon className="h-4 w-4 text-gray-500" />
            </button>
          </div>
        )}
      </div>

      {/* Widget Content */}
      <div className="p-4 h-[calc(100%-3.5rem)]">
        {data.loading ? (
          <div className="flex items-center justify-center h-full">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
          </div>
        ) : data.error ? (
          <div className="flex items-center justify-center h-full">
            <div className="text-sm text-red-600 text-center">
              <p className="font-medium">Error loading data</p>
              <p className="text-xs mt-1">{data.error}</p>
            </div>
          </div>
        ) : (
          children
        )}
      </div>
    </div>
  );
};