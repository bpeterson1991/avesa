import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { WidgetConfig, WidgetLayout, DashboardLayout } from '../types/widget';

interface WidgetState {
  // Current dashboard layout
  currentLayout: DashboardLayout | null;
  
  // Available layouts
  layouts: DashboardLayout[];
  
  // Widget data cache
  widgetData: Record<string, any>;
  
  // Edit mode state
  isEditMode: boolean;
  
  // Selected widget for editing
  selectedWidgetId: string | null;
  
  // Legacy support for simpler API
  dashboards: Record<string, DashboardLayout>;
  activeDashboard: string;
  
  // Actions
  setCurrentLayout: (layout: DashboardLayout) => void;
  addWidget: (widget: WidgetConfig, layout: WidgetLayout) => void;
  updateWidget: (widgetId: string, updates: Partial<WidgetConfig>) => void;
  removeWidget: (widgetId: string) => void;
  updateLayouts: (layouts: DashboardLayout['layouts']) => void;
  setWidgetData: (widgetId: string, data: any) => void;
  setEditMode: (isEdit: boolean) => void;
  setSelectedWidget: (widgetId: string | null) => void;
  saveLayout: () => void;
  loadLayout: (layoutId: string) => void;
  createNewLayout: (name: string) => void;
  deleteLayout: (layoutId: string) => void;
  resetDashboard: () => void;
}

const defaultLayout: DashboardLayout = {
  id: 'default',
  name: 'Default Dashboard',
  widgets: [],
  layouts: {
    lg: [],
    md: [],
    sm: [],
    xs: []
  },
  isDefault: true,
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString()
};

export const useWidgetStore = create<WidgetState>()(
  persist(
    (set, get) => ({
      currentLayout: defaultLayout,
      layouts: [defaultLayout],
      widgetData: {},
      isEditMode: false,
      selectedWidgetId: null,
      dashboards: { default: defaultLayout },
      activeDashboard: 'default',

      setCurrentLayout: (layout) => set({ currentLayout: layout }),

      addWidget: (widget, layout) => set((state) => {
        if (!state.currentLayout) return state;
        
        // Generate widget id if not provided
        const widgetId = widget.id || `widget-${Date.now()}`;
        const newWidget = { ...widget, id: widgetId };
        const newLayouts = { ...state.currentLayout.layouts };

        // Add layout for all breakpoints
        Object.keys(newLayouts).forEach((breakpoint) => {
          const newLayout = {
            ...layout,
            i: newWidget.id
          };
          
          // Adjust layout for different breakpoints
          if (breakpoint === 'md') {
            newLayout.w = Math.min(layout.w * 2, 8);
          } else if (breakpoint === 'sm') {
            newLayout.w = Math.min(layout.w * 2, 4);
            newLayout.x = 0;
          } else if (breakpoint === 'xs') {
            newLayout.w = 4;
            newLayout.x = 0;
          }
          
          newLayouts[breakpoint as keyof typeof newLayouts].push(newLayout);
        });

        const updatedLayout = {
          ...state.currentLayout,
          widgets: [...state.currentLayout.widgets, newWidget],
          layouts: newLayouts,
          updatedAt: new Date().toISOString()
        };

        // Update in layouts array
        const updatedLayouts = state.layouts.map(l => 
          l.id === updatedLayout.id ? updatedLayout : l
        );

        return {
          currentLayout: updatedLayout,
          layouts: updatedLayouts,
          dashboards: { ...state.dashboards, [state.activeDashboard]: updatedLayout }
        };
      }),

      updateWidget: (widgetId, updates) => set((state) => {
        if (!state.currentLayout) return state;

        const updatedWidgets = state.currentLayout.widgets.map(w =>
          w.id === widgetId ? { ...w, ...updates } : w
        );

        const updatedLayout = {
          ...state.currentLayout,
          widgets: updatedWidgets,
          updatedAt: new Date().toISOString()
        };

        const updatedLayouts = state.layouts.map(l =>
          l.id === updatedLayout.id ? updatedLayout : l
        );

        return {
          currentLayout: updatedLayout,
          layouts: updatedLayouts,
          dashboards: { ...state.dashboards, [state.activeDashboard]: updatedLayout }
        };
      }),

      removeWidget: (widgetId) => set((state) => {
        if (!state.currentLayout) return state;

        const updatedWidgets = state.currentLayout.widgets.filter(w => w.id !== widgetId);
        const newLayouts = { ...state.currentLayout.layouts };

        // Remove from all breakpoint layouts
        Object.keys(newLayouts).forEach((breakpoint) => {
          newLayouts[breakpoint as keyof typeof newLayouts] = 
            newLayouts[breakpoint as keyof typeof newLayouts].filter(l => l.i !== widgetId);
        });

        const updatedLayout = {
          ...state.currentLayout,
          widgets: updatedWidgets,
          layouts: newLayouts,
          updatedAt: new Date().toISOString()
        };

        const updatedLayouts = state.layouts.map(l =>
          l.id === updatedLayout.id ? updatedLayout : l
        );

        // Clean up widget data
        const newWidgetData = { ...state.widgetData };
        delete newWidgetData[widgetId];

        return {
          currentLayout: updatedLayout,
          layouts: updatedLayouts,
          widgetData: newWidgetData,
          dashboards: { ...state.dashboards, [state.activeDashboard]: updatedLayout }
        };
      }),

      updateLayouts: (layouts) => set((state) => {
        if (!state.currentLayout) return state;

        const updatedLayout = {
          ...state.currentLayout,
          layouts,
          updatedAt: new Date().toISOString()
        };

        const updatedLayouts = state.layouts.map(l =>
          l.id === updatedLayout.id ? updatedLayout : l
        );

        return {
          currentLayout: updatedLayout,
          layouts: updatedLayouts,
          dashboards: { ...state.dashboards, [state.activeDashboard]: updatedLayout }
        };
      }),

      setWidgetData: (widgetId, data) => set((state) => ({
        widgetData: {
          ...state.widgetData,
          [widgetId]: data
        }
      })),

      setEditMode: (isEdit) => set({ isEditMode: isEdit, selectedWidgetId: null }),

      setSelectedWidget: (widgetId) => set({ selectedWidgetId: widgetId }),

      saveLayout: () => {
        const state = get();
        if (!state.currentLayout) return;
        
        // In a real app, this would save to the backend
        console.log('Layout saved:', state.currentLayout);
      },

      loadLayout: (layoutId) => set((state) => {
        const layout = state.layouts.find(l => l.id === layoutId);
        if (layout) {
          return { currentLayout: layout };
        }
        return state;
      }),

      createNewLayout: (name) => set((state) => {
        const newLayout: DashboardLayout = {
          id: `layout-${Date.now()}`,
          name,
          widgets: [],
          layouts: {
            lg: [],
            md: [],
            sm: [],
            xs: []
          },
          isDefault: false,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString()
        };

        return {
          layouts: [...state.layouts, newLayout],
          currentLayout: newLayout
        };
      }),

      deleteLayout: (layoutId) => set((state) => {
        if (state.layouts.length <= 1) return state; // Keep at least one layout

        const updatedLayouts = state.layouts.filter(l => l.id !== layoutId);
        const newCurrentLayout = state.currentLayout?.id === layoutId 
          ? updatedLayouts[0] 
          : state.currentLayout;

        return {
          layouts: updatedLayouts,
          currentLayout: newCurrentLayout,
          dashboards: state.dashboards
        };
      }),

      resetDashboard: () => set((state) => {
        // Clear current widgets
        const resetLayout: DashboardLayout = {
          ...defaultLayout,
          widgets: [],
          layouts: {
            lg: [],
            md: [],
            sm: [],
            xs: []
          },
          updatedAt: new Date().toISOString()
        };

        const updatedLayouts = state.layouts.map(l =>
          l.id === state.currentLayout?.id ? resetLayout : l
        );

        return {
          currentLayout: resetLayout,
          layouts: updatedLayouts,
          dashboards: { ...state.dashboards, [state.activeDashboard]: resetLayout },
          widgetData: {}
        };
      })
    }),
    {
      name: 'widget-store',
      partialize: (state) => ({
        layouts: state.layouts,
        currentLayout: state.currentLayout
      })
    }
  )
);