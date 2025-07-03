import React from 'react';

interface WidgetCanvasProps {
  children: React.ReactNode;
  isEditMode: boolean;
}

export const WidgetCanvas: React.FC<WidgetCanvasProps> = ({ children, isEditMode }) => {
  return (
    <div className={`h-full relative overflow-auto p-4 ${
      isEditMode ? 'bg-gray-100' : 'bg-gray-50'
    }`}>
      {isEditMode && (
        <div className="absolute inset-0 bg-grid-pattern opacity-5 pointer-events-none" />
      )}
      <div className="relative z-10 max-w-screen-2xl mx-auto">
        {children}
      </div>
    </div>
  );
};