import React, { Fragment } from 'react';
import { Dialog, Transition } from '@headlessui/react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { useWidgetStore } from '../../stores/widgetStore';
import { WidgetLayout } from '../../types/widget';
import { 
  ChartBarIcon, 
  ChartPieIcon, 
  PresentationChartLineIcon,
  Square3Stack3DIcon,
  TableCellsIcon
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

interface WidgetLibraryProps {
  isOpen: boolean;
  onClose: () => void;
}

const widgetTemplates = [
  {
    type: 'kpi-card' as const,
    title: 'KPI Card',
    description: 'Display a single key metric with optional trend',
    icon: Square3Stack3DIcon,
    defaultConfig: {
      visualization: {
        type: 'kpi-card',
        options: {
          format: 'number'
        }
      }
    }
  },
  {
    type: 'chart' as const,
    title: 'Bar Chart',
    description: 'Compare values across categories',
    icon: ChartBarIcon,
    defaultConfig: {
      visualization: {
        type: 'bar',
        options: {
          xAxisKey: 'name',
          dataKeys: ['value'],
          height: 300
        }
      }
    }
  },
  {
    type: 'chart' as const,
    title: 'Line Chart',
    description: 'Show trends over time',
    icon: PresentationChartLineIcon,
    defaultConfig: {
      visualization: {
        type: 'line',
        options: {
          xAxisKey: 'date',
          dataKeys: ['value'],
          height: 300
        }
      }
    }
  },
  {
    type: 'chart' as const,
    title: 'Pie Chart',
    description: 'Show distribution of values',
    icon: ChartPieIcon,
    defaultConfig: {
      visualization: {
        type: 'pie',
        options: {
          yAxisKey: 'value',
          height: 300
        }
      }
    }
  },
  {
    type: 'table' as const,
    title: 'Data Table',
    description: 'Display data in a tabular format',
    icon: TableCellsIcon,
    defaultConfig: {
      visualization: {
        type: 'table',
        options: {
          pageSize: 10
        }
      }
    }
  }
];

export const WidgetLibrary: React.FC<WidgetLibraryProps> = ({ isOpen, onClose }) => {
  const { addWidget } = useWidgetStore();

  const handleAddWidget = (template: typeof widgetTemplates[0]) => {
    const id = `widget-${Date.now()}`;
    const title = `New ${template.title}`;
    const dataKey = `data-${id}`;
    
    // Find an empty spot on the grid
    const layout: WidgetLayout = {
      i: id,
      w: 4,
      h: 4,
      x: 0,
      y: 0
    };

    addWidget({
      id,
      type: template.type,
      title,
      dataKey,
      visualization: template.defaultConfig.visualization,
      config: template.defaultConfig
    }, layout);

    toast.success(`Added ${template.title} widget`);
    onClose();
  };

  return (
    <Transition.Root show={isOpen} as={Fragment}>
      <Dialog as="div" className="relative z-50" onClose={onClose}>
        <Transition.Child
          as={Fragment}
          enter="ease-out duration-300"
          enterFrom="opacity-0"
          enterTo="opacity-100"
          leave="ease-in duration-200"
          leaveFrom="opacity-100"
          leaveTo="opacity-0"
        >
          <div className="fixed inset-0 bg-gray-500 bg-opacity-75 transition-opacity" />
        </Transition.Child>

        <div className="fixed inset-0 z-10 overflow-y-auto">
          <div className="flex min-h-full items-end justify-center p-4 text-center sm:items-center sm:p-0">
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-300"
              enterFrom="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
              enterTo="opacity-100 translate-y-0 sm:scale-100"
              leave="ease-in duration-200"
              leaveFrom="opacity-100 translate-y-0 sm:scale-100"
              leaveTo="opacity-0 translate-y-4 sm:translate-y-0 sm:scale-95"
            >
              <Dialog.Panel className="relative transform overflow-hidden rounded-lg bg-white px-4 pb-4 pt-5 text-left shadow-xl transition-all sm:my-8 sm:w-full sm:max-w-2xl sm:p-6">
                <div className="absolute right-0 top-0 hidden pr-4 pt-4 sm:block">
                  <button
                    type="button"
                    className="rounded-md bg-white text-gray-400 hover:text-gray-500 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:ring-offset-2"
                    onClick={onClose}
                  >
                    <span className="sr-only">Close</span>
                    <XMarkIcon className="h-6 w-6" aria-hidden="true" />
                  </button>
                </div>
                <div className="sm:flex sm:items-start">
                  <div className="mt-3 text-center sm:ml-4 sm:mt-0 sm:text-left w-full">
                    <Dialog.Title as="h3" className="text-base font-semibold leading-6 text-gray-900">
                      Widget Library
                    </Dialog.Title>
                    <div className="mt-2">
                      <p className="text-sm text-gray-500">
                        Choose a widget type to add to your dashboard
                      </p>
                    </div>
                    <div className="mt-6 grid grid-cols-1 gap-4 sm:grid-cols-2">
                      {widgetTemplates.map((template) => (
                        <button
                          key={`${template.type}-${template.title}`}
                          onClick={() => handleAddWidget(template)}
                          className="relative rounded-lg border border-gray-300 bg-white p-4 shadow-sm hover:border-gray-400 focus:outline-none focus:ring-2 focus:ring-primary-500"
                        >
                          <div className="flex items-start">
                            <div className="flex-shrink-0">
                              <template.icon className="h-6 w-6 text-gray-400" />
                            </div>
                            <div className="ml-3 text-left">
                              <h4 className="text-sm font-medium text-gray-900">{template.title}</h4>
                              <p className="mt-1 text-sm text-gray-500">{template.description}</p>
                            </div>
                          </div>
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
              </Dialog.Panel>
            </Transition.Child>
          </div>
        </div>
      </Dialog>
    </Transition.Root>
  );
};